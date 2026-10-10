"""External-process configuration. Never bundled into a QMT strategy."""
from dataclasses import dataclass, field
import os
import re


@dataclass(frozen=True)
class Config:
    host: str = "127.0.0.1"
    port: int = 8888
    bridge_id: str = "default"
    account_id: str = "66027616"
    pg_config: dict | None = field(default=None, repr=False)
    request_timeout: int = 10
    download_timeout: int = 120
    log_level: str = "INFO"
    log_directory: str = "var/log/ipc_bridge"
    settings: dict = field(default_factory=lambda: dict(
        submit_batch_size=10, cancel_batch_size=10, reconcile_batch_size=100,
        schedule_budget_ms=50, reconcile_interval_seconds=30))

    def pipe_name(self, role):
        if role not in ("feed", "order"):
            raise ValueError("invalid IPC role")
        return "\\\\.\\pipe\\qmt-bridge.{}.{}".format(self.bridge_id, role)

    @classmethod
    def from_env(cls, environ=None, dotenv_path=".env"):
        if environ is None:
            from dotenv import dotenv_values
            values = {**dotenv_values(dotenv_path), **os.environ}
        else:
            values = dict(environ)

        def value(key, default):
            return values.get("QMT_BRIDGE_" + key, default)

        def integer(key, default, maximum=2147483647):
            raw = value(key, str(default))
            if not isinstance(raw, str) or not raw.isascii() or not raw.isdecimal():
                raise ValueError("QMT_BRIDGE_" + key + " must be a positive integer")
            number = int(raw)
            if not 1 <= number <= maximum:
                raise ValueError("QMT_BRIDGE_" + key + " is outside its valid range")
            return number

        bridge_id = value("ID", "default")
        if not isinstance(bridge_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", bridge_id):
            raise ValueError("QMT_BRIDGE_ID must contain 1..64 letters, digits, '_' or '-'")
        account = value("ACCOUNT_ID", "66027616")
        if not isinstance(account, str) or not account.strip():
            raise ValueError("QMT_BRIDGE_ACCOUNT_ID must be a nonempty string")
        if "QMT_BRIDGE_PG_SCHEMA" in values:
            raise ValueError("database schema is fixed to qmt_order")
        pg = None
        keys = ("PG_DATABASE", "PG_USER", "PG_PASSWORD")
        if any(value(key, "") for key in keys):
            if any(not isinstance(value(key, None), str) or not value(key, "") for key in keys):
                raise ValueError("set QMT_BRIDGE_PG_DATABASE, PG_USER and PG_PASSWORD together")
            pg = {"pg_host": value("PG_HOST", "127.0.0.1"),
                  "pg_port": integer("PG_PORT", 5432, 65535),
                  "pg_database": value("PG_DATABASE", None),
                  "pg_user": value("PG_USER", None),
                  "pg_password": value("PG_PASSWORD", None), "pg_schema": "qmt_order"}
        settings = {key: integer(key.upper(), default) for key, default in
                    cls().settings.items()}
        host = value("HOST", "127.0.0.1")
        if not isinstance(host, str) or not host.strip():
            raise ValueError("QMT_BRIDGE_HOST must be nonempty")
        level = value("LOG_LEVEL", "INFO").upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError("invalid QMT_BRIDGE_LOG_LEVEL")
        return cls(host=host, port=integer("PORT", 8888, 65535),
                   bridge_id=bridge_id, account_id=account, pg_config=pg,
                   request_timeout=integer("REQUEST_TIMEOUT", 10),
                   download_timeout=integer("DOWNLOAD_TIMEOUT", 120),
                   log_level=level, log_directory=value("LOG_DIRECTORY", "var/log/ipc_bridge"),
                   settings=settings)
