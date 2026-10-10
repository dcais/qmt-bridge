"""Run from the repository root: python -m service.ipc_bridge."""
from .config import Config
import argparse
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description="Start the QMT IPC bridge.")
    parser.add_argument("--env-file", type=Path,
                        help="Configuration file, for example .env.staging or .env.prd (default: .env)")
    args = parser.parse_args(argv)
    if args.env_file is not None and not args.env_file.is_file():
        parser.error("environment file not found: {}".format(args.env_file))
    config = Config.from_env(dotenv_path=args.env_file if args.env_file is not None else ".env")
    import uvicorn
    from .app import create_app
    # Exactly one owner of the pipe instances and database executor.
    uvicorn.run(create_app(config), host=config.host, port=config.port,
                workers=1, log_level=config.log_level.lower())


if __name__ == "__main__":
    main()
