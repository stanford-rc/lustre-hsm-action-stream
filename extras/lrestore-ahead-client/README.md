# Lustre HSM Restore-Ahead Client

A daemon for intelligent, paced "restore-ahead" operations on Lustre HSM.

- **Requires root privileges** to run `lfs` commands.
- Install dependencies from `requirements.txt`
- Configure your environment by editing `lrestore_ahead_client.yaml`.
- Run manually: `python3 lrestore_ahead_client.py -c your_config.yaml`
- Deploy as a service using the included `lrestore-ahead-client.service` template.
- Metrics are exposed on port 5003 by default (see YAML file).
