#!/usr/bin/env python3
"""Remote Backup: restic backups of Home Assistant data to SFTP, S3 or any restic backend."""
import collections
import json
import os
import re
import shlex
import socket
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Flask, abort, jsonify, request, send_from_directory

VERSION = "1.2.2"
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("DATA_DIR", "/data")
SSH_DIR = os.path.join(DATA, "ssh")
KEY = os.path.join(SSH_DIR, "id_ed25519")
KNOWN_HOSTS = os.path.join(SSH_DIR, "known_hosts")
SETTINGS_FILE = os.path.join(DATA, "settings.json")
STATE_FILE = os.path.join(DATA, "state.json")
LOG_DIR = os.path.join(DATA, "logs")
RESTORE_ROOT = os.environ.get("RESTORE_ROOT", "/share/remote-backup-restore")
INGRESS_IP = "172.30.32.2"
ROOTS = ["/config", "/ssl", "/media", "/backup", "/share", "/addon_configs"]
SNAPSHOT_RE = re.compile(r"[0-9a-f]{8,64}")
BLOCKED_ENV = {"PATH", "SUPERVISOR_TOKEN", "RESTIC_PASSWORD", "RESTIC_CACHE_DIR"}

# Presets shown in the UI. "type" decides which settings fields apply.
PROVIDERS = {
    "hetzner": {
        "label": "Hetzner Storage Box", "type": "sftp", "port": 23, "path": "ha-backup",
        "host_hint": "u123456.your-storagebox.de", "user_hint": "u123456", "key_install": "hetzner",
        "note": "Enable SSH support for the Storage Box in the Hetzner console first. SFTP uses port 23.",
    },
    "hostinger": {
        "label": "Hostinger (web hosting or VPS)", "type": "sftp", "port": 65002, "path": "backups/homeassistant",
        "host_hint": "Server IP shown in hPanel > SSH Access", "user_hint": "u123456789", "key_install": "shell",
        "note": "Enable SSH access in hPanel. Web/cloud hosting uses port 65002, a VPS uses 22. "
                "Backups count against your plan's disk quota.",
    },
    "sftp": {
        "label": "Any SSH / SFTP server (VPS, NAS, ...)", "type": "sftp", "port": 22, "path": "ha-backup",
        "host_hint": "server.example.com", "user_hint": "backup", "key_install": "shell",
        "note": "Any server that offers SFTP. A path starting with / is absolute, otherwise it is relative to the login home.",
    },
    "s3": {
        "label": "S3-compatible (AWS, Backblaze B2, Wasabi, MinIO, ...)", "type": "s3", "port": 0, "path": "ha-backup",
        "note": "Endpoint example: s3.eu-central-003.backblazeb2.com or s3.amazonaws.com. Use an access key limited to this bucket.",
    },
    "custom": {
        "label": "Advanced: any restic repository", "type": "custom", "port": 0, "path": "",
        "note": "Enter a restic repository string (rest:, rclone:, azure:, gs:, b2:, ...) and its environment variables. "
                "rclone is installed, so rclone: works with RCLONE_CONFIG_<NAME>_* variables.",
    },
}

DEFAULTS = {
    "provider": "hetzner",
    "host": "",
    "user": "",
    "port": 23,
    "path": "ha-backup",
    "s3_endpoint": "",
    "s3_bucket": "",
    "s3_region": "",
    "s3_access_key": "",
    "s3_secret_key": "",
    "repository": "",
    "extra_env": "",
    "passphrase": "",
    "sources": ["/config"],
    "excludes": [
        "home-assistant_v2.db*",
        "*.log",
        "*.log.*",
        "__pycache__",
        "/config/tts",
        "/config/deps",
    ],
    "compression": "auto",
    "schedule": {"enabled": False, "mode": "daily", "time": "03:00", "every_hours": 24},
    "retention": {"daily": 7, "weekly": 4, "monthly": 6},
    "notify_failure": True,
}

app = Flask(__name__)


@app.before_request
def only_ingress():
    if os.environ.get("DEV"):
        return
    if request.remote_addr not in (INGRESS_IP, "127.0.0.1"):
        abort(403)


# ---------------------------------------------------------------- settings / state

def load_settings():
    s = json.loads(json.dumps(DEFAULTS))
    try:
        with open(SETTINGS_FILE) as f:
            saved = json.load(f)
    except (OSError, ValueError):
        saved = {}
    for k, v in saved.items():
        if isinstance(v, dict) and isinstance(s.get(k), dict):
            s[k].update(v)
        else:
            s[k] = v
    if s["provider"] not in PROVIDERS:
        s["provider"] = "sftp"
    return s


def under_root(p, real=False):
    """Normalize p and return it if it lies inside a mounted root, else None."""
    p = os.path.normpath("/" + str(p).strip().lstrip("/"))
    for root in ROOTS:
        if p == root or p.startswith(root + "/"):
            if real:
                rp, rr = os.path.realpath(p), os.path.realpath(root)
                if not (rp == rr or rp.startswith(rr + os.sep)):
                    return None
            return p
    return None


def write_private(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def parse_env(text):
    env = {}
    for line in str(text).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, val = line.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Z_][A-Z0-9_]*", key):
            raise ValueError(f"Bad environment line: {line.split('=')[0][:30]}")
        if key in BLOCKED_ENV or key.startswith("LD_"):
            raise ValueError(f"{key} cannot be set here")
        env[key] = val.strip()
    return env


def clean_settings(p, cur):
    s = json.loads(json.dumps(cur))

    def text(key, rx, label):
        v = str(p.get(key, s[key])).strip()
        if v and not re.fullmatch(rx, v):
            raise ValueError(f"Invalid {label}")
        s[key] = v

    s["provider"] = p.get("provider", s["provider"])
    if s["provider"] not in PROVIDERS:
        raise ValueError("Unknown provider")
    text("host", r"[A-Za-z0-9.-]+", "host")
    text("user", r"[A-Za-z0-9._-]+", "user")
    text("path", r"[A-Za-z0-9._/-]+", "path")
    if ".." in s["path"].split("/"):
        raise ValueError("Invalid path")
    s["port"] = int(p.get("port", s["port"]) or 0)
    text("s3_endpoint", r"(https?://)?[A-Za-z0-9.:-]+", "S3 endpoint")
    text("s3_bucket", r"[A-Za-z0-9._-]+", "S3 bucket")
    text("s3_region", r"[A-Za-z0-9-]+", "S3 region")
    text("s3_access_key", r"[A-Za-z0-9/+=_.-]+", "S3 access key")
    text("repository", r"\S+", "repository")
    if s["repository"].startswith("-"):
        raise ValueError("Invalid repository")
    if "extra_env" in p:
        parse_env(p["extra_env"])
        s["extra_env"] = str(p["extra_env"])
    for secret in ("passphrase", "s3_secret_key"):
        if p.get(secret):
            s[secret] = str(p[secret])
    if re.search(r"\s", s["s3_secret_key"]):
        raise ValueError("Invalid S3 secret key")

    s["sources"] = []
    for x in p.get("sources", cur["sources"]):
        q = under_root(x)
        if q is None:
            raise ValueError(f"Not a mounted folder: {str(x)[:80]}")
        if q not in s["sources"]:
            s["sources"].append(q)
    s["excludes"] = [str(x).strip() for x in p.get("excludes", s["excludes"]) if str(x).strip()]
    s["compression"] = p.get("compression", s["compression"])
    if s["compression"] not in ("auto", "max", "off"):
        raise ValueError("Invalid compression")

    sch = p.get("schedule", {})
    cs = s["schedule"]
    cs["enabled"] = bool(sch.get("enabled", cs["enabled"]))
    cs["mode"] = sch.get("mode", cs["mode"])
    if cs["mode"] not in ("daily", "interval"):
        raise ValueError("Invalid schedule mode")
    cs["time"] = str(sch.get("time", cs["time"]))
    m = re.fullmatch(r"(\d{2}):(\d{2})", cs["time"])
    if not m or int(m[1]) > 23 or int(m[2]) > 59:
        raise ValueError("Invalid schedule time")
    cs["every_hours"] = max(1, min(720, int(sch.get("every_hours", cs["every_hours"]))))

    ret = p.get("retention", {})
    for k in ("daily", "weekly", "monthly"):
        s["retention"][k] = max(0, min(9999, int(ret.get(k, s["retention"][k]))))
    s["notify_failure"] = bool(p.get("notify_failure", s["notify_failure"]))
    return s


STATE_LOCK = threading.RLock()


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def update_state(fn):
    with STATE_LOCK:
        st = load_state()
        fn(st)
        write_private(STATE_FILE, json.dumps(st))


def ptype(s):
    return PROVIDERS[s["provider"]]["type"]


def missing_fields(s):
    need = {"passphrase": "Encryption passphrase"}
    t = ptype(s)
    if t == "sftp":
        need.update(host="Host", user="Username", port="SSH port", path="Folder on server")
    elif t == "s3":
        need.update(s3_endpoint="Endpoint", s3_bucket="Bucket", s3_access_key="Access key", s3_secret_key="Secret key")
    else:
        need["repository"] = "Restic repository"
    return [label for key, label in need.items() if not s[key]]


def configured(s):
    return not missing_fields(s)


# ---------------------------------------------------------------- ssh / restic plumbing

def ensure_key():
    os.makedirs(SSH_DIR, mode=0o700, exist_ok=True)
    if not os.path.exists(KEY):
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-N", "", "-C", "ha-remote-backup", "-f", KEY],
            check=True, capture_output=True,
        )


def pubkey():
    try:
        with open(KEY + ".pub") as f:
            return f.read().strip()
    except OSError:
        return ""


def ssh_base(s):
    return [
        "ssh", "-p", str(s["port"]), "-i", KEY,
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"UserKnownHostsFile={KNOWN_HOSTS}",
        "-o", "ServerAliveInterval=30",
        "-o", "ConnectTimeout=15",
    ]


def repo_spec(s):
    """Return (restic repository string, extra restic args, extra env)."""
    t = ptype(s)
    if t == "sftp":
        target = f"{s['user']}@{s['host']}"
        cmd = " ".join(shlex.quote(a) for a in ssh_base(s) + [target, "-s", "sftp"])
        return f"sftp:{target}:{s['path']}", ["-o", f"sftp.command={cmd}"], {}
    if t == "s3":
        endpoint = s["s3_endpoint"].rstrip("/")
        if not endpoint.startswith(("http://", "https://")):
            endpoint = "https://" + endpoint
        repo = f"s3:{endpoint}/{s['s3_bucket']}"
        if s["path"]:
            repo += "/" + s["path"].strip("/")
        env = {"AWS_ACCESS_KEY_ID": s["s3_access_key"], "AWS_SECRET_ACCESS_KEY": s["s3_secret_key"]}
        if s["s3_region"]:
            env["AWS_DEFAULT_REGION"] = s["s3_region"]
        return repo, [], env
    return s["repository"], [], parse_env(s["extra_env"])


def restic(s, *args):
    repo, extra, _ = repo_spec(s)
    return ["restic", "-r", repo, *extra, *args]


def restic_env(s):
    env = os.environ.copy()
    env.update(repo_spec(s)[2])
    env["RESTIC_PASSWORD"] = s["passphrase"]
    env["RESTIC_CACHE_DIR"] = os.path.join(DATA, "restic-cache")
    return env


def notify(title, message):
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return
    body = json.dumps({"title": title, "message": message, "notification_id": "remote_backup"})
    req = urllib.request.Request(
        "http://supervisor/core/api/services/persistent_notification/create",
        data=body.encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:  # notification is best-effort
        print("notify failed:", e)


# ---------------------------------------------------------------- jobs

CRED_RE = re.compile(r"(://[^:/@\s]+):[^@\s]+@")


class Job:
    def __init__(self, kind, trigger):
        self.id = int(time.time() * 1000)
        self.kind = kind
        self.trigger = trigger
        self.start = time.time()
        self.end = None
        self.ok = None
        self.summary = ""
        self.lines = []
        self.out = ""  # full output of the most recent run()

    def log(self, msg):
        if len(self.lines) < 50000:
            self.lines.append(str(msg).rstrip("\r\n"))

    def run(self, cmd, env, cwd=None):
        self.log(CRED_RE.sub(r"\1:***@", "$ " + " ".join(shlex.quote(c) for c in cmd)))
        p = subprocess.Popen(
            cmd, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace",
        )
        out = []
        for line in p.stdout:
            self.log(line)
            out.append(line)
        self.out = "".join(out)
        return p.wait()

    def public(self):
        return {"id": self.id, "kind": self.kind, "trigger": self.trigger,
                "start": self.start, "end": self.end, "ok": self.ok, "summary": self.summary}


RUN_LOCK = threading.Lock()
CURRENT = None
LAST = None


def start_job(kind, fn, trigger="manual"):
    global CURRENT
    if not RUN_LOCK.acquire(blocking=False):
        return None
    job = Job(kind, trigger)
    CURRENT = job
    threading.Thread(target=run_job, args=(job, fn), daemon=True).start()
    return job


def save_log(job):
    lines = job.lines
    if len(lines) > 5000:
        lines = [f"... {len(lines) - 5000} earlier lines omitted ..."] + lines[-5000:]
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        write_private(os.path.join(LOG_DIR, f"{job.id}.log"), "\n".join(lines) + "\n")
    except OSError as e:
        print("could not save log:", e)


def drop_log(job_id):
    try:
        os.remove(os.path.join(LOG_DIR, f"{job_id}.log"))
    except OSError:
        pass


def run_job(job, fn):
    global CURRENT, LAST
    try:
        ok, summary = fn(job)
    except Exception as e:
        job.log(f"Internal error: {e}")
        ok, summary = False, f"Internal error: {e}"
    job.end, job.ok, job.summary = time.time(), ok, summary
    entry = job.public()
    save_log(job)

    def add(st):
        st.setdefault("history", []).insert(0, entry)
        for old in st["history"][50:]:
            drop_log(old["id"])
        del st["history"][50:]

    update_state(add)
    if not ok and job.kind == "backup" and load_settings()["notify_failure"]:
        notify("Remote backup failed", summary)
    LAST, CURRENT = job, None
    RUN_LOCK.release()


def not_ready(job):
    job.log("Finish the destination settings and set a passphrase first.")
    return False, "Not configured"


# (regex, status, plain-language explanation); first match wins, so specific rules come first.
HINTS = [
    (r"REMOTE HOST IDENTIFICATION HAS CHANGED|Host key verification failed", "fail",
     "The server's host key differs from the one saved earlier. If the server was reinstalled this is expected: "
     "press 'Forget saved host key' (SSH key section) and test again."),
    (r"Could not resolve hostname|Name or service not known|no such host|Temporary failure in name resolution|nodename nor servname", "fail",
     "The host name cannot be resolved. Check its spelling and that Home Assistant has internet/DNS access."),
    (r"timed out|i/o timeout", "fail",
     "The server did not answer. Check the port, that SSH/SFTP is enabled at your provider, and that no firewall blocks it."),
    (r"Connection refused", "fail",
     "Nothing is listening on this port. Check the port number and that SSH/SFTP is enabled at your provider."),
    (r"Permission denied \(|subprocess ssh:.*Permission denied|Too many authentication failures|no supported authentication", "fail",
     "The server rejected the SSH key. Install the public key (Settings > SSH key > Install key) and check the username."),
    (r"sftp connections only|shell access is disabled", "warn",
     "Shell commands are disabled on this account (normal for SFTP-only accounts). Backups can still work."),
    (r"subsystem request failed", "fail",
     "The server accepted the login but does not offer SFTP for this account. Ask your provider to enable SFTP."),
    (r"wrong password or no key found|ciphertext verification failed", "fail",
     "Wrong encryption passphrase for this repository."),
    (r"InvalidAccessKeyId|SignatureDoesNotMatch|AccessDenied|Forbidden|\b40[13]\b|Unauthorized", "fail",
     "The storage rejected the credentials. Check the access key, secret key and the key's permissions on the bucket."),
    (r"NoSuchBucket", "fail", "The bucket does not exist. Check the bucket name, endpoint and region."),
    (r"already locked|unable to create lock", "fail",
     "Another backup holds a lock on the repository, or one crashed. Wait for it to finish, or press 'Unlock repository' to remove stale locks."),
    (r"Is there a repository at the following location|unable to open config file|repository does not exist|file does not exist|no such file or directory", "warn",
     "No repository exists at this location yet. If this is your first setup press 'Initialize repository'; otherwise check the folder path."),
    (r"Permission denied", "fail",
     "The account is not allowed to use this folder. Check the folder path and its permissions."),
]


def classify(text):
    for rx, status, hint in HINTS:
        if re.search(rx, text, re.I):
            return status, hint
    return None, ""


def tail(text, n=4):
    lines = [x for x in text.strip().splitlines() if x.strip()]
    return "\n".join(lines[-n:])[:500]


def net_target(s):
    t = ptype(s)
    if t == "sftp":
        return s["host"], s["port"]
    if t == "s3":
        ep = s["s3_endpoint"]
        u = urlparse(ep if "://" in ep else "https://" + ep)
        return u.hostname, u.port or (80 if u.scheme == "http" else 443)
    return None, None


def job_test(job):
    s = load_settings()
    steps = []

    def step(name, status, detail="", hint=""):
        steps.append({"name": name, "status": status, "detail": detail, "hint": hint})
        job.log(f"[{status.upper()}] {name}" + (f": {detail}" if detail else "") + (f"\n    -> {hint}" if hint else ""))

    def finish(summary):
        update_state(lambda st: st.__setitem__("last_test", {"at": time.time(), "steps": steps}))
        return all(x["status"] != "fail" for x in steps), summary

    if not configured(s):
        step("Settings", "fail", "Missing: " + ", ".join(missing_fields(s)),
             "Fill in these fields and press 'Save settings' (the check uses the saved values, not the form).")
        return finish("Not configured")

    host, port = net_target(s)
    if host:
        try:
            ips = sorted({a[4][0] for a in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
            step("Find server", "ok", f"{host} -> {', '.join(ips[:3])}")
        except OSError as e:
            step("Find server", "fail", str(e), classify(str(e))[1] or "The host name cannot be resolved. Check its spelling.")
            return finish("Host not found")
        try:
            socket.create_connection((host, port), timeout=8).close()
            step("Open connection", "ok", f"port {port} is reachable")
        except OSError as e:
            step("Open connection", "fail", f"{host}:{port} - {e}",
                 classify(str(e))[1] or "Could not connect. Check the port and your provider's firewall.")
            return finish("Cannot reach server")

    if ptype(s) == "sftp":
        rc = job.run(ssh_base(s) + [f"{s['user']}@{s['host']}", "ls"], os.environ.copy())
        status, hint = classify(job.out)
        if rc == 0:
            step("SSH login", "ok", "Logged in with the SSH key")
        elif status == "fail":
            step("SSH login", "fail", tail(job.out), hint)
            return finish("SSH login failed")
        else:
            step("SSH login", "warn", tail(job.out),
                 hint or "The shell login did not work; trying the SFTP repository check anyway.")

    rc = job.run(restic(s, "cat", "config"), restic_env(s))
    status, hint = classify(job.out)
    if rc == 0:
        step("Repository", "ok", "Repository found and passphrase accepted")
        return finish("Connection OK, repository found")
    if status == "warn":
        step("Repository", "warn", tail(job.out), hint)
        return finish("Reachable, but no repository yet - initialize it")
    step("Repository", "fail", tail(job.out),
         hint or "restic could not open the repository. See the detail above and the activity log.")
    return finish("Repository check failed")


def job_init(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    rc = job.run(restic(s, "init", "--repository-version", "2"), restic_env(s))
    return rc == 0, "Repository initialized" if rc == 0 else f"restic init failed (exit {rc})"


def unlock(job, s):
    """Remove stale locks left behind by an interrupted job (live locks are kept)."""
    return job.run(restic(s, "unlock"), restic_env(s))


def job_unlock(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    rc = unlock(job, s)
    return rc == 0, "Stale locks removed" if rc == 0 else f"restic unlock failed (exit {rc})"


def prune(job, s):
    r = s["retention"]
    keep = []
    for k in ("daily", "weekly", "monthly"):
        if r[k] > 0:
            keep += [f"--keep-{k}", str(r[k])]
    if not keep:
        job.log("Retention disabled; skipping prune.")
        return 0
    cmd = restic(s, "forget", "--prune", "--tag", "homeassistant", "--host", "homeassistant",
                 "--group-by", "host,tags", *keep)
    return job.run(cmd, restic_env(s))


def job_backup(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    srcs = [p for p in s["sources"] if os.path.exists(p)]
    for p in s["sources"]:
        if p not in srcs:
            job.log(f"Skipping missing path: {p}")
    if not srcs:
        job.log("No backup sources selected (or none exist).")
        return False, "No sources selected"
    unlock(job, s)
    cmd = restic(s, "backup", "--tag", "homeassistant", "--host", "homeassistant",
                 "--compression", s["compression"], "--exclude-caches", "--exclude", RESTORE_ROOT)
    for pat in s["excludes"]:
        cmd += ["--exclude", pat]
    rc = job.run(cmd + srcs, restic_env(s))
    if rc not in (0, 3):  # 3 = snapshot created but some files unreadable
        return False, f"restic backup failed (exit {rc})"
    summary = "Snapshot created" + (" (some files unreadable)" if rc == 3 else "")
    job.log("Pruning old snapshots...")
    if prune(job, s) != 0:
        return False, summary + "; prune failed"
    return True, summary


def job_prune(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    unlock(job, s)
    rc = prune(job, s)
    return rc == 0, "Pruned old snapshots" if rc == 0 else f"restic forget failed (exit {rc})"


def make_restore(snap, paths):
    def fn(job):
        s = load_settings()
        if not configured(s):
            return not_ready(job)
        dest = os.path.join(RESTORE_ROOT, snap)
        os.makedirs(dest, exist_ok=True)
        cmd = restic(s, "restore", snap, "--target", dest)
        for p in paths:
            cmd += ["--include", "/" + p]
        rc = job.run(cmd, restic_env(s))
        return rc == 0, f"Restored to {dest}" if rc == 0 else f"restic restore failed (exit {rc})"
    return fn


def make_delete(snap):
    def fn(job):
        s = load_settings()
        if not configured(s):
            return not_ready(job)
        rc = job.run(restic(s, "forget", "--prune", snap), restic_env(s))
        return rc == 0, f"Deleted {snap}" if rc == 0 else f"restic forget failed (exit {rc})"
    return fn


# ---------------------------------------------------------------- scheduler

def next_run(s, st):
    sch = s["schedule"]
    if not sch["enabled"] or not configured(s):
        return None
    last = st.get("last_scheduled", 0)
    if sch["mode"] == "interval":
        return max(time.time(), last + sch["every_hours"] * 3600) if last else time.time()
    h, m = map(int, sch["time"].split(":"))
    now = datetime.now()
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if last >= target.timestamp():
        target += timedelta(days=1)
    return target.timestamp()


def scheduler():
    while True:
        time.sleep(30)
        try:
            s = load_settings()
            nxt = next_run(s, load_state())
            if nxt is not None and time.time() >= nxt:
                if start_job("backup", job_backup, trigger="schedule"):
                    update_state(lambda st: st.__setitem__("last_scheduled", time.time()))
        except Exception as e:
            print("scheduler error:", e)


# ---------------------------------------------------------------- routes

@app.get("/")
def index():
    return send_from_directory(os.path.join(APP_DIR, "static"), "index.html")


@app.get("/api/state")
def api_state():
    s = load_settings()
    st = load_state()
    safe = dict(s, passphrase="", s3_secret_key="",
                passphrase_set=bool(s["passphrase"]), s3_secret_set=bool(s["s3_secret_key"]))
    return jsonify(
        version=VERSION,
        settings=safe,
        providers=PROVIDERS,
        configured=configured(s),
        pubkey=pubkey(),
        job=CURRENT.public() if CURRENT else None,
        history=st.get("history", [])[:15],
        next_run=next_run(s, st),
        last_test=st.get("last_test"),
        restore_root=RESTORE_ROOT,
    )


@app.post("/api/hostkey/reset")
def api_hostkey_reset():
    try:
        os.remove(KNOWN_HOSTS)
    except FileNotFoundError:
        pass
    return jsonify(ok=True)


@app.get("/api/fs")
def api_fs():
    path = request.args.get("path", "")
    if not path.strip("/"):
        roots = [{"name": r.lstrip("/"), "path": r, "type": "d", "size": None} for r in ROOTS if os.path.isdir(r)]
        return jsonify(path="", entries=roots)
    p = under_root(path, real=True)
    if p is None or not os.path.isdir(p):
        return jsonify(error="Not a mounted folder"), 400
    entries = []
    try:
        with os.scandir(p) as it:
            for e in it:
                if len(entries) >= 3000:
                    break
                try:
                    is_dir = e.is_dir(follow_symlinks=False)
                    size = None if is_dir else e.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
                entries.append({"name": e.name, "path": f"{p}/{e.name}", "type": "d" if is_dir else "f", "size": size})
    except OSError as err:
        return jsonify(error=str(err)), 500
    entries.sort(key=lambda e: (e["type"] != "d", e["name"].lower()))
    return jsonify(path=p, entries=entries)


@app.get("/api/log")
def api_log():
    job = CURRENT or LAST
    if not job:
        return jsonify(job_id=None, running=False, lines=[], next=0)
    try:
        since = int(request.args.get("since", 0))
    except ValueError:
        since = 0
    if request.args.get("job") != str(job.id):
        since = 0
    lines = job.lines[since:]
    return jsonify(job_id=str(job.id), running=job is CURRENT, lines=lines, next=since + len(lines))


@app.get("/api/history/<int:job_id>/log")
def api_history_log(job_id):
    if not any(x["id"] == job_id for x in load_state().get("history", [])):
        return jsonify(error="Unknown history entry"), 404
    try:
        with open(os.path.join(LOG_DIR, f"{job_id}.log"), errors="replace") as f:
            return jsonify(text=f.read())
    except OSError:
        return jsonify(error="No log was saved for this entry"), 404


@app.post("/api/settings")
def api_settings():
    try:
        s = clean_settings(request.get_json(force=True) or {}, load_settings())
    except (ValueError, TypeError) as e:
        return jsonify(error=str(e)), 400
    write_private(SETTINGS_FILE, json.dumps(s))
    REPO_CACHE["data"] = None
    TREE["archive"] = None
    return jsonify(ok=True)


@app.post("/api/key/install")
def api_key_install():
    s = load_settings()
    pw = (request.get_json(force=True) or {}).get("password", "")
    if ptype(s) != "sftp":
        return jsonify(error="SSH keys only apply to SFTP destinations"), 400
    if not (s["host"] and s["user"]):
        return jsonify(error="Save host and user first"), 400
    if not pw:
        return jsonify(error="Enter the account password"), 400
    if PROVIDERS[s["provider"]]["key_install"] == "hetzner":
        remote = "install-ssh-key"
    else:
        remote = "umask 077; mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys"
    env = os.environ.copy()
    env["SSHPASS"] = pw
    cmd = ["sshpass", "-e", "ssh", "-p", str(s["port"]),
           "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={KNOWN_HOSTS}",
           "-o", "PreferredAuthentications=password", "-o", "PubkeyAuthentication=no",
           f"{s['user']}@{s['host']}", remote]
    try:
        with open(KEY + ".pub") as pub:
            r = subprocess.run(cmd, stdin=pub, env=env, capture_output=True, text=True, timeout=45)
    except subprocess.TimeoutExpired:
        return jsonify(error="Timed out. Is SSH access enabled on the server?"), 500
    out = (r.stdout + r.stderr).strip()
    if r.returncode != 0:
        return jsonify(error=out or f"ssh exited with {r.returncode}"), 500
    return jsonify(ok=True, output=out)


@app.post("/api/job/<kind>")
def api_job(kind):
    fns = {"test": job_test, "init": job_init, "backup": job_backup, "prune": job_prune, "unlock": job_unlock}
    if kind not in fns:
        abort(404)
    job = start_job(kind, fns[kind])
    if not job:
        return jsonify(error="Another job is already running"), 409
    return jsonify(ok=True)


REPO_CACHE = {"at": 0, "data": None}


@app.get("/api/repo")
def api_repo():
    s = load_settings()
    if not configured(s):
        return jsonify(ready=False)
    if not request.args.get("refresh") and REPO_CACHE["data"] and time.time() - REPO_CACHE["at"] < 60:
        return jsonify(REPO_CACHE["data"])
    env = restic_env(s)
    try:
        r = subprocess.run(restic(s, "snapshots", "--json"), env=env, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            return jsonify(ready=True, error=r.stderr.strip()[-600:])
        snaps = [{"id": x["short_id"], "time": x["time"], "paths": x.get("paths", [])}
                 for x in json.loads(r.stdout or "[]") or []]
        snaps.sort(key=lambda x: x["time"], reverse=True)
        size = None
        r = subprocess.run(restic(s, "stats", "--mode", "raw-data", "--json"), env=env,
                           capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            size = json.loads(r.stdout).get("total_size")
    except (subprocess.TimeoutExpired, ValueError, KeyError) as e:
        return jsonify(ready=True, error=f"Could not read repository: {e}")
    data = {"ready": True, "snapshots": snaps, "size": size}
    REPO_CACHE.update(at=time.time(), data=data)
    return jsonify(data)


TREE = {"archive": None, "children": {}}


def load_tree(s, snap):
    children = collections.defaultdict(dict)
    p = subprocess.Popen(
        restic(s, "ls", "--json", snap), env=restic_env(s),
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, errors="replace",
    )
    for line in p.stdout:
        try:
            node = json.loads(line)
        except ValueError:
            continue
        if node.get("struct_type") != "node":
            continue
        parts = node["path"].strip("/").split("/")
        is_dir = node.get("type") == "dir"
        for i, name in enumerate(parts):
            parent = "/".join(parts[:i])
            if i < len(parts) - 1 or is_dir:
                children[parent][name] = "d"
            else:
                children[parent].setdefault(name, "f")
    if p.wait() != 0:
        raise RuntimeError("restic ls failed")
    TREE.update(archive=snap, children=children)


@app.get("/api/browse")
def api_browse():
    snap = request.args.get("archive", "")
    path = request.args.get("path", "").strip("/")
    if not SNAPSHOT_RE.fullmatch(snap):
        return jsonify(error="Bad snapshot id"), 400
    s = load_settings()
    if not configured(s):
        return jsonify(error="Not configured"), 400
    if TREE["archive"] != snap:
        try:
            load_tree(s, snap)
        except RuntimeError as e:
            return jsonify(error=str(e)), 500
    entries = [{"name": n, "type": t} for n, t in TREE["children"].get(path, {}).items()]
    entries.sort(key=lambda e: (e["type"] != "d", e["name"].lower()))
    return jsonify(path=path, entries=entries)


@app.post("/api/restore")
def api_restore():
    body = request.get_json(force=True) or {}
    snap, paths = body.get("archive", ""), body.get("paths", [])
    if not SNAPSHOT_RE.fullmatch(snap) or not paths:
        return jsonify(error="Pick a snapshot and at least one path"), 400
    for p in paths:
        if not isinstance(p, str) or p.startswith("/") or ".." in p.split("/"):
            return jsonify(error=f"Bad path: {p}"), 400
    if not start_job("restore", make_restore(snap, paths)):
        return jsonify(error="Another job is already running"), 409
    return jsonify(ok=True)


@app.post("/api/delete")
def api_delete():
    snap = (request.get_json(force=True) or {}).get("archive", "")
    if not SNAPSHOT_RE.fullmatch(snap):
        return jsonify(error="Bad snapshot id"), 400
    if not start_job("delete", make_delete(snap)):
        return jsonify(error="Another job is already running"), 409
    REPO_CACHE["data"] = None
    return jsonify(ok=True)


if __name__ == "__main__":
    os.makedirs(DATA, exist_ok=True)
    ensure_key()
    threading.Thread(target=scheduler, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8099)), threaded=True)
