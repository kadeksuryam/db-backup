"""Email notification backend using stdlib smtplib."""

from __future__ import annotations

import smtplib
from email.mime.text import MIMEText

from config import ConfigError
from notifiers import Notifier


class EmailNotifier(Notifier):
    """Send notifications via SMTP email."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        username: str = "",
        password: str = "",
        from_addr: str = "",
        to_addr: str = "",
        use_tls: bool = True,
        subject_prefix: str = "[dbbackup]",
        timeout: float = 30.0,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_addr = from_addr
        self.to_addr = to_addr
        self.use_tls = use_tls
        self.subject_prefix = subject_prefix
        self.timeout = timeout

    def send(self, job_name: str, status: str, message: str) -> None:
        subject = f"{self.subject_prefix} {job_name}: {status.upper()}"
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = self.from_addr
        msg["To"] = self.to_addr

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.timeout) as server:
            if self.use_tls:
                server.starttls()
            if self.username:
                server.login(self.username, self.password)
            server.sendmail(self.from_addr, [self.to_addr], msg.as_string())


def create(config: dict) -> EmailNotifier:
    """Create an EmailNotifier from a config dict."""
    if "smtp_host" not in config:
        raise ConfigError("Error: email notifier config is missing required 'smtp_host' field")
    return EmailNotifier(
        smtp_host=config["smtp_host"],
        smtp_port=int(config.get("smtp_port", 587)),
        username=config.get("username", ""),
        password=config.get("password", ""),
        from_addr=config.get("from", ""),
        to_addr=config.get("to", ""),
        use_tls=config.get("use_tls", True),
        subject_prefix=config.get("subject_prefix", "[dbbackup]"),
        timeout=float(config.get("timeout", 30)),
    )
