# Remote Backup

Backs up selected Home Assistant folders to a remote location with [restic](https://restic.net):
encrypted before upload, deduplicated, versioned. Managed from the sidebar panel.

## Supported destinations

| Provider | How it connects | Needs on the server |
|---|---|---|
| Hetzner Storage Box | SFTP, port 23 | SSH support enabled in the Hetzner console |
| Hostinger (web hosting / VPS) | SFTP, port 65002 (hosting) or 22 (VPS) | SSH access enabled in hPanel |
| Any SSH/SFTP server | SFTP, your port | An account with SFTP |
| S3-compatible (AWS, Backblaze B2, Wasabi, MinIO...) | S3 API | A bucket and access key |
| Advanced | any restic repository string (`rest:`, `rclone:`, `azure:`, `gs:`...) | depends |

Nothing has to be installed on the remote side: restic only needs plain SFTP or S3.
For Google Drive, OneDrive, Dropbox and similar, use *Advanced* with an `rclone:` repository and
`RCLONE_CONFIG_<NAME>_*` environment variables.

## First-time setup

1. Open the panel > **Settings**, pick your provider and fill in the destination. **Save**.
2. SFTP only: under **SSH key**, click *Install key* with the account password (used once, never stored),
   or add the shown public key to `authorized_keys` / your provider's panel yourself.
3. Set an **encryption passphrase** and keep a copy outside Home Assistant. Without it backups cannot be restored.
4. Choose folders, schedule and retention. **Save**.
5. **Test connection**, then **Initialize repository** (once).
6. On Overview, click **Back up now**.

## Restoring

Snapshots tab > Browse a snapshot > tick files/folders > Restore.
Files land in `/share/remote-backup-restore/<snapshot>/` (visible via the Samba share add-on).
Live config is never overwritten; copy files back yourself.

On a fresh install: install the add-on, enter the same destination and passphrase, skip *Initialize*,
and browse the existing snapshots.

## Notes

- `/config` is mounted read-only; the add-on cannot modify your configuration.
- The HA database (`home-assistant_v2.db*`) is excluded by default. Remove the pattern to include it.
- SFTP host keys are trusted on first use and stored in the add-on's private data.
- Failed scheduled backups raise a persistent notification in Home Assistant.
