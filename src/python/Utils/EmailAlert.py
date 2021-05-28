"""
EmailAlert - send alerts via email

NOTICE:
This class does not work from kubernetes pods. The AlertManagerAPI class should be used for alerting from k8s.
More details at: https://github.com/dmwm/WMCore/issues/10234
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

        try:
            # clean up smtp connection
            smtp.quit()
        except UnboundLocalError:
            # it means our client failed connecting to the SMTP server
            pass

