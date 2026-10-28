# Storage Box Backup

Backs up selected Home Assistant folders to a Hetzner Storage Box with BorgBackup:
encrypted, deduplicated, versioned. Everything is managed from the sidebar panel.

## First-time setup

1. In the Hetzner console, enable **SSH support** for your Storage Box.
2. Open the add-on panel > **Settings**. Enter host (`uXXXXXX.your-storagebox.de`) and username, then **Save**.
3. **SSH key**: click *Install key* with your Storage Box password (used once, never stored),
   or paste the shown public key into the Storage Box's SSH keys in the Hetzner console.
4. Set an **encryption passphrase** and keep a copy outside Home Assistant.
5. Choose sources, schedule and retention, then **Save**.
6. **Test connection**, then **Initialize repository** (once).
7. On the Overview tab, click **Back up now**.

## Restoring

Snapshots tab > Browse a snapshot > tick files/folders > Restore.
Files land in `/share/storagebox-restore/<snapshot>/` (visible via the Samba share add-on).
Live config is never overwritten; copy files back yourself.

To restore on a fresh install: install this add-on, enter the same settings and passphrase,
skip *Initialize*, and browse the existing snapshots.

## Notes

- `/config` is mounted read-only. The add-on cannot modify your configuration.
- The HA database (`home-assistant_v2.db*`) is excluded by default. Remove the pattern to include it.
- `borg-1.2` is the default remote Borg path; adjust it if Hetzner lists a newer version.
- Failed scheduled backups raise a persistent notification in Home Assistant.
