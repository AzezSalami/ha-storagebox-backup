#!/usr/bin/env python3
"""Storage Box Backup: BorgBackup of Home Assistant data to a Hetzner Storage Box."""
import collections
import json
import os
import re
import shlex
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timedelta

from flask import Flask, abort, jsonify, request, send_from_directory

VERSION = "0.1.0"
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("DATA_DIR", "/data")
SSH_DIR = os.path.join(DATA, "ssh")
KEY = os.path.join(SSH_DIR, "id_ed25519")
KNOWN_HOSTS = os.path.join(SSH_DIR, "known_hosts")
SETTINGS_FILE = os.path.join(DATA, "settings.json")
STATE_FILE = os.path.join(DATA, "state.json")
RESTORE_ROOT = os.environ.get("RESTORE_ROOT", "/share/storagebox-restore")
INGRESS_IP = "172.30.32.2"
SOURCES = {"config": "/config", "ssl": "/ssl", "media": "/media", "backup": "/backup", "share": "/share"}
ARCHIVE_RE = re.compile(r"[A-Za-z0-9._:-]+")

DEFAULTS = {
    "host": "",
    "user": "",
    "port": 23,
    "borg_remote": "borg-1.2",
    "repo_dir": "ha-backup",
    "passphrase": "",
    "sources": ["config"],
    "excludes": [
        "*/home-assistant_v2.db*",
        "*.log",
        "*.log.*",
        "*/__pycache__",
        "*/tts",
        "*/deps",
    ],
    "compression": "zstd,3",
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
    return s


def write_private(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def clean_settings(p, cur):
    s = json.loads(json.dumps(cur))

    def text(key, rx, label):
        v = str(p.get(key, s[key])).strip()
        if v and not re.fullmatch(rx, v):
            raise ValueError(f"Invalid {label}")
        s[key] = v

    text("host", r"[A-Za-z0-9.-]+", "host")
    text("user", r"[A-Za-z0-9._-]+", "user")
    text("borg_remote", r"[A-Za-z0-9._-]+", "Borg remote path")
    text("repo_dir", r"[A-Za-z0-9._/-]+", "repository folder")
    text("compression", r"[a-z0-9,]+", "compression")
    if ".." in s["repo_dir"].split("/"):
        raise ValueError("Invalid repository folder")
    s["port"] = int(p.get("port", s["port"]))
    if p.get("passphrase"):
        s["passphrase"] = str(p["passphrase"])
    s["sources"] = [k for k in p.get("sources", s["sources"]) if k in SOURCES]
    s["excludes"] = [str(x).strip() for x in p.get("excludes", s["excludes"]) if str(x).strip()]

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


def configured(s):
    return bool(s["host"] and s["user"] and s["repo_dir"] and s["passphrase"])


# ---------------------------------------------------------------- ssh / borg plumbing

def ensure_key():
    os.makedirs(SSH_DIR, mode=0o700, exist_ok=True)
    if not os.path.exists(KEY):
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-N", "", "-C", "ha-storagebox-backup", "-f", KEY],
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
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"UserKnownHostsFile={KNOWN_HOSTS}",
        "-o", "ConnectTimeout=15",
    ]


def repo_url(s):
    return f"ssh://{s['user']}@{s['host']}:{s['port']}/./{s['repo_dir']}"


def borg_env(s):
    env = os.environ.copy()
    env["BORG_PASSPHRASE"] = s["passphrase"]
    env["BORG_BASE_DIR"] = os.path.join(DATA, "borg")
    env["BORG_RSH"] = " ".join(shlex.quote(a) for a in ssh_base(s)[:1] + [
        "-i", KEY, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"UserKnownHostsFile={KNOWN_HOSTS}", "-o", "ServerAliveInterval=30",
    ])
    env["BORG_RELOCATED_REPO_ACCESS_IS_OK"] = "yes"
    return env


def borg(s, *args):
    return ["borg", "--remote-path", s["borg_remote"], *args]


def notify(title, message):
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return
    body = json.dumps({"title": title, "message": message, "notification_id": "storagebox_backup"})
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

    def log(self, msg):
        if len(self.lines) < 50000:
            self.lines.append(str(msg).rstrip("\r\n"))

    def run(self, cmd, env, cwd=None):
        self.log("$ " + " ".join(shlex.quote(c) for c in cmd))
        p = subprocess.Popen(
            cmd, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace",
        )
        for line in p.stdout:
            self.log(line)
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


def run_job(job, fn):
    global CURRENT, LAST
    try:
        ok, summary = fn(job)
    except Exception as e:
        job.log(f"Internal error: {e}")
        ok, summary = False, f"Internal error: {e}"
    job.end, job.ok, job.summary = time.time(), ok, summary
    entry = job.public()

    def add(st):
        st.setdefault("history", []).insert(0, entry)
        del st["history"][50:]

    update_state(add)
    if not ok and job.kind == "backup" and load_settings()["notify_failure"]:
        notify("Storage Box backup failed", summary)
    LAST, CURRENT = job, None
    RUN_LOCK.release()


def not_ready(job):
    job.log("Fill in host, user, repository folder and passphrase in Settings first.")
    return False, "Not configured"


def job_test(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    job.log("Checking SSH login...")
    rc = job.run(ssh_base(s) + ["-o", "BatchMode=yes", f"{s['user']}@{s['host']}", "ls"], os.environ.copy())
    if rc != 0:
        return False, "SSH login failed - install the public key first (Settings > SSH key)"
    job.log("Checking Borg repository...")
    rc = job.run(borg(s, "info", repo_url(s)), borg_env(s))
    if rc == 0:
        return True, "Connection OK, repository found"
    return True, "SSH works, but no usable repository - initialize it, or check the passphrase"


def job_init(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    rc = job.run(borg(s, "init", "--encryption=repokey-blake2", repo_url(s)), borg_env(s))
    if rc == 0:
        return True, "Repository initialized"
    return False, f"borg init failed (exit {rc})"


def prune(job, s):
    r = s["retention"]
    keep = []
    for k in ("daily", "weekly", "monthly"):
        if r[k] > 0:
            keep += [f"--keep-{k}", str(r[k])]
    if not keep:
        job.log("Retention disabled; skipping prune.")
        return 0
    env = borg_env(s)
    rc = job.run(borg(s, "prune", "--list", "--glob-archives", "ha-*", *keep, repo_url(s)), env)
    if rc <= 1:
        job.run(borg(s, "compact", repo_url(s)), env)  # best effort, needs borg >= 1.2
    return rc


def job_backup(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    srcs = [SOURCES[k] for k in s["sources"] if os.path.isdir(SOURCES[k])]
    if not srcs:
        job.log("No backup sources selected (or none are mounted).")
        return False, "No sources selected"
    name = datetime.now().strftime("ha-%Y-%m-%d_%H-%M")
    cmd = borg(s, "create", "--stats", "--compression", s["compression"],
               "--exclude-caches", "--exclude", RESTORE_ROOT)
    for pat in s["excludes"]:
        cmd += ["--exclude", pat]
    cmd += [f"{repo_url(s)}::{name}", *srcs]
    rc = job.run(cmd, borg_env(s))
    if rc > 1:
        return False, f"borg create failed (exit {rc})"
    summary = f"Created {name}" + (" (with warnings)" if rc == 1 else "")
    job.log("Pruning old snapshots...")
    if prune(job, s) > 1:
        summary += "; prune failed"
        return False, summary
    return True, summary


def job_prune(job):
    s = load_settings()
    if not configured(s):
        return not_ready(job)
    rc = prune(job, s)
    return rc <= 1, "Pruned old snapshots" if rc <= 1 else f"borg prune failed (exit {rc})"


def make_restore(archive, paths):
    def fn(job):
        s = load_settings()
        if not configured(s):
            return not_ready(job)
        dest = os.path.join(RESTORE_ROOT, archive)
        os.makedirs(dest, exist_ok=True)
        cmd = borg(s, "extract", "--list", f"{repo_url(s)}::{archive}", *paths)
        rc = job.run(cmd, borg_env(s), cwd=dest)
        if rc > 1:
            return False, f"borg extract failed (exit {rc})"
        return True, f"Restored to {dest}"
    return fn


def make_delete(archive):
    def fn(job):
        s = load_settings()
        if not configured(s):
            return not_ready(job)
        rc = job.run(borg(s, "delete", f"{repo_url(s)}::{archive}"), borg_env(s))
        return rc <= 1, f"Deleted {archive}" if rc <= 1 else f"borg delete failed (exit {rc})"
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
    safe = dict(s, passphrase="", passphrase_set=bool(s["passphrase"]))
    return jsonify(
        version=VERSION,
        settings=safe,
        configured=configured(s),
        pubkey=pubkey(),
        job=CURRENT.public() if CURRENT else None,
        history=st.get("history", [])[:15],
        next_run=next_run(s, st),
        sources={k: os.path.isdir(v) for k, v in SOURCES.items()},
        restore_root=RESTORE_ROOT,
    )


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


@app.post("/api/settings")
def api_settings():
    try:
        s = clean_settings(request.get_json(force=True) or {}, load_settings())
    except (ValueError, TypeError) as e:
        return jsonify(error=str(e)), 400
    write_private(SETTINGS_FILE, json.dumps(s))
    return jsonify(ok=True)


@app.post("/api/key/install")
def api_key_install():
    s = load_settings()
    pw = (request.get_json(force=True) or {}).get("password", "")
    if not (s["host"] and s["user"]):
        return jsonify(error="Save host and user in Settings first"), 400
    if not pw:
        return jsonify(error="Enter the Storage Box password"), 400
    env = os.environ.copy()
    env["SSHPASS"] = pw
    cmd = ["sshpass", "-e", "ssh", "-p", str(s["port"]),
           "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={KNOWN_HOSTS}",
           "-o", "PreferredAuthentications=password", "-o", "PubkeyAuthentication=no",
           f"{s['user']}@{s['host']}", "install-ssh-key"]
    try:
        with open(KEY + ".pub") as pub:
            r = subprocess.run(cmd, stdin=pub, env=env, capture_output=True, text=True, timeout=45)
    except subprocess.TimeoutExpired:
        return jsonify(error="Timed out. Is SSH support enabled on the Storage Box?"), 500
    out = (r.stdout + r.stderr).strip()
    if r.returncode != 0:
        return jsonify(error=out or f"ssh exited with {r.returncode}"), 500
    return jsonify(ok=True, output=out)


@app.post("/api/job/<kind>")
def api_job(kind):
    fns = {"test": job_test, "init": job_init, "backup": job_backup, "prune": job_prune}
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
    env, url = borg_env(s), repo_url(s)
    try:
        r = subprocess.run(borg(s, "list", "--json", url), env=env, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            return jsonify(ready=True, error=r.stderr.strip()[-600:])
        archives = [{"name": a["name"], "start": a["start"]} for a in json.loads(r.stdout)["archives"]]
        archives.sort(key=lambda a: a["start"], reverse=True)
        size = None
        r = subprocess.run(borg(s, "info", "--json", url), env=env, capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            size = json.loads(r.stdout).get("cache", {}).get("stats", {}).get("unique_csize")
    except (subprocess.TimeoutExpired, ValueError, KeyError) as e:
        return jsonify(ready=True, error=f"Could not read repository: {e}")
    data = {"ready": True, "archives": archives, "size": size}
    REPO_CACHE.update(at=time.time(), data=data)
    return jsonify(data)


TREE = {"archive": None, "children": {}}


def load_tree(s, archive):
    children = collections.defaultdict(dict)
    p = subprocess.Popen(
        borg(s, "list", "--format", "{type}{path}{NL}", f"{repo_url(s)}::{archive}"),
        env=borg_env(s), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, errors="replace",
    )
    for line in p.stdout:
        line = line.rstrip("\n")
        if len(line) < 2:
            continue
        kind, path = line[0], line[1:]
        parts = path.split("/")
        for i, name in enumerate(parts):
            parent = "/".join(parts[:i])
            if i < len(parts) - 1 or kind == "d":
                children[parent][name] = "d"
            else:
                children[parent].setdefault(name, "f")
    if p.wait() != 0:
        raise RuntimeError("borg list failed")
    TREE.update(archive=archive, children=children)


@app.get("/api/browse")
def api_browse():
    archive = request.args.get("archive", "")
    path = request.args.get("path", "").strip("/")
    if not ARCHIVE_RE.fullmatch(archive):
        return jsonify(error="Bad archive name"), 400
    s = load_settings()
    if not configured(s):
        return jsonify(error="Not configured"), 400
    if TREE["archive"] != archive:
        try:
            load_tree(s, archive)
        except RuntimeError as e:
            return jsonify(error=str(e)), 500
    entries = [{"name": n, "type": t} for n, t in TREE["children"].get(path, {}).items()]
    entries.sort(key=lambda e: (e["type"] != "d", e["name"].lower()))
    return jsonify(path=path, entries=entries)


@app.post("/api/restore")
def api_restore():
    body = request.get_json(force=True) or {}
    archive, paths = body.get("archive", ""), body.get("paths", [])
    if not ARCHIVE_RE.fullmatch(archive) or not paths:
        return jsonify(error="Pick an archive and at least one path"), 400
    for p in paths:
        if not isinstance(p, str) or p.startswith(("/", "-")) or ".." in p.split("/"):
            return jsonify(error=f"Bad path: {p}"), 400
    if not start_job("restore", make_restore(archive, paths)):
        return jsonify(error="Another job is already running"), 409
    return jsonify(ok=True)


@app.post("/api/delete")
def api_delete():
    archive = (request.get_json(force=True) or {}).get("archive", "")
    if not ARCHIVE_RE.fullmatch(archive):
        return jsonify(error="Bad archive name"), 400
    if not start_job("delete", make_delete(archive)):
        return jsonify(error="Another job is already running"), 409
    REPO_CACHE["data"] = None
    return jsonify(ok=True)


@app.get("/api/key/export")
def api_key_export():
    s = load_settings()
    if not configured(s):
        return jsonify(error="Not configured"), 400
    r = subprocess.run(borg(s, "key", "export", repo_url(s), "-"), env=borg_env(s),
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        return jsonify(error=r.stderr.strip()[-400:]), 500
    return jsonify(key=r.stdout)


if __name__ == "__main__":
    os.makedirs(DATA, exist_ok=True)
    ensure_key()
    threading.Thread(target=scheduler, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8099)), threaded=True)
