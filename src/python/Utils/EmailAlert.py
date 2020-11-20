"""
EmailAlert - send alerts via email
"""

from __future__ import division
from builtins import str, object
import smtplib
import logging


class EmailAlert(object):
    """
    A simple class to send alerts via email
    """

    EMAIL_HEADER = "From: %s\r\nSubject: %s\r\nTo: %s\r\n\r\n"

    def __init__(self, configDict):
        self.serverName = configDict.get("smtpServer", "localhost")
        self.fromAddr = configDict.get("fromAddr", "noreply@cern.ch")
        self.toAddr = configDict.get("toAddr", "cms-service-production-admins@cern.ch")
        if not isinstance(self.toAddr, (list, set)):
            self.toAddr = [self.toAddr]

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

