"""Tests for notifiers package."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from config import ConfigError
from notifiers import create_notifier
from notifiers.email import EmailNotifier


class TestNotifierFactory:
    def test_create_email_notifier(self):
        cfg = {"type": "email", "smtp_host": "smtp.test", "smtp_port": 587}
        notifier = create_notifier(cfg)
        assert isinstance(notifier, EmailNotifier)

    def test_unknown_type_raises(self):
        with pytest.raises(ConfigError, match="Unknown notifier type"):
            create_notifier({"type": "slack"})


class TestEmailNotifierFactory:
    def test_email_missing_smtp_host_raises_config_error(self):
        """email create() without 'smtp_host' → ConfigError."""
        from notifiers.email import create
        with pytest.raises(ConfigError, match="smtp_host"):
            create({"smtp_port": 587, "from": "a@b.com"})


class TestEmailNotifier:
    @patch("notifiers.email.smtplib.SMTP")
    def test_email_send_success(self, mock_smtp_class):
        mock_server = MagicMock()
        mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

        notifier = EmailNotifier(
            smtp_host="smtp.test",
            smtp_port=587,
            username="user",
            password="pass",
            from_addr="from@test.com",
            to_addr="to@test.com",
            use_tls=True,
        )
        notifier.send("myjob", "success", "Backup done")

        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")
        mock_server.sendmail.assert_called_once()
        args = mock_server.sendmail.call_args[0]
        assert args[0] == "from@test.com"
        assert args[1] == ["to@test.com"]

    @patch("notifiers.email.smtplib.SMTP")
    def test_email_send_no_tls(self, mock_smtp_class):
        mock_server = MagicMock()
        mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

        notifier = EmailNotifier(
            smtp_host="smtp.test",
            use_tls=False,
            from_addr="a@b.com",
            to_addr="c@d.com",
        )
        notifier.send("job1", "failure", "Error")

        mock_server.starttls.assert_not_called()

    @patch("notifiers.email.smtplib.SMTP")
    def test_email_send_no_auth(self, mock_smtp_class):
        mock_server = MagicMock()
        mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

        notifier = EmailNotifier(
            smtp_host="smtp.test",
            username="",
            from_addr="a@b.com",
            to_addr="c@d.com",
        )
        notifier.send("job1", "success", "Done")

        mock_server.login.assert_not_called()

    @patch("notifiers.email.smtplib.SMTP")
    def test_email_subject_format(self, mock_smtp_class):
        mock_server = MagicMock()
        mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

        notifier = EmailNotifier(
            smtp_host="smtp.test",
            from_addr="a@b.com",
            to_addr="c@d.com",
            subject_prefix="[backup]",
        )
        notifier.send("myjob", "failure", "Error occurred")

        sent_msg = mock_server.sendmail.call_args[0][2]
        assert "[backup] myjob: FAILURE" in sent_msg

    @patch("notifiers.email.smtplib.SMTP")
    def test_email_smtp_failure_raises(self, mock_smtp_class):
        mock_smtp_class.side_effect = OSError("Connection refused")

        notifier = EmailNotifier(smtp_host="bad.host", from_addr="a@b.com", to_addr="c@d.com")
        with pytest.raises(OSError, match="Connection refused"):
            notifier.send("job1", "failure", "Error")

    @patch("notifiers.email.smtplib.SMTP")
    def test_email_timeout_passed_to_smtp(self, mock_smtp_class):
        """timeout parameter is passed to smtplib.SMTP constructor."""
        mock_server = MagicMock()
        mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

        notifier = EmailNotifier(
            smtp_host="smtp.test",
            from_addr="a@b.com",
            to_addr="c@d.com",
            timeout=15.0,
        )
        notifier.send("job1", "success", "Done")

        mock_smtp_class.assert_called_once_with("smtp.test", 587, timeout=15.0)
