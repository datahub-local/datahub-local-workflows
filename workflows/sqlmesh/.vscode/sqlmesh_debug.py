import os

from sqlmesh.cli.main import cli

# Enable the local gateway when running in VS Code debug mode.
# Without this, only the homelab gateway is loaded and default_gateway is homelab.
os.environ.setdefault("SQLMESH_LOCAL_GATEWAY", "true")

if __name__ == "__main__":
    raise SystemExit(cli())