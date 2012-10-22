"""
EmailSink - send alerts via email.

"""

import logging
import smtplib



class EmailSink(object):
    """
    Alert sink to dispatch alerts by email.

    """


    EMAIL_HEADER = "From: %s\r\nSubject: %s\r\nTo: %s\r\n\r\n"

    def __init__(self, config):
        self.config = config
        logging.info("Instantiating ...")
        self.serverName = getattr(self.config, "smtpServer", "localhost")
        self.smtp = smtplib.SMTP(self.serverName)
        # produces a lot of debug output, uncomment in case of email delivery issues
        # (pymox tests needs to know about this possible call, fails otherwise)
        # self.smtp.set_debuglevel(1)
        login, passw = getattr(config, "smtpUser", None), getattr(config, "smtpPass", None)
        if login != None:
            self.smtp.login(login, passw)
        self.fromAddr = getattr(self.config, "fromAddr", None)
        self.toAddr  = getattr(self.config, "toAddr", None)
        logging.info("Initialized.")


    def send(self, alerts):
        """
        Handle list of alerts.

        """
        subj = "Alert from %s" % alerts[0]["HostName"]
        msg = self.EMAIL_HEADER % (self.fromAddr, subj, ", ".join(self.toAddr))
        for a in alerts:
            msg += "\n%s\n" % a.toMsg()
        try:
            self.smtp.sendmail(self.fromAddr, self.toAddr, msg)
        except smtplib.SMTPServerDisconnected:
            logging.warn("Server disconnected, reconnecting ...")
            self.smtp = smtplib.SMTP(self.serverName)
            self.smtp.sendmail(self.fromAddr, self.toAddr, msg)
        logging.debug("Sent %s alerts." % len(alerts))


    def __del__(self):
        if hasattr(self, "smtp"):
            self.smtp.quit()
