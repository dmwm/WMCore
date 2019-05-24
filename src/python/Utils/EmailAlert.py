"""
EmailAlert - send alerts via email
"""

from __future__ import division
import smtplib
import logging


class EmailAlert:
    """
    A simple class to send alerts via email
    """

    EMAIL_HEADER = "From: %s\r\nSubject: %s\r\nTo: %s\r\n\r\n"

    def __init__(self, config):
        self.config = config
        self.serverName = getattr(config.EmailAlert, "smtpServer")
        self.fromAddr = getattr(config.EmailAlert, "fromAddr")
        self.toAddr = getattr(config.EmailAlert, "toAddr")

    def send(self, subject, message):
        """
        Send an email

        :param subject: Email subject
        :param message: Email body

        """
        msg = self.EMAIL_HEADER % (self.fromAddr, subject, ", ".join(self.toAddr))
        msg += message

        try:
            smtp = smtplib.SMTP(self.serverName)
            smtp.sendmail(self.fromAddr, self.toAddr, msg)
        except Exception as ex:
            logging.exception("Error sending alert email.\nDetails: %s", str(ex))
        finally:
            # clean up smtp connection
            smtp.quit()

