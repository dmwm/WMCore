"""
EmailSink - send alerts via email.

"""

import logging
import smtplib



class EmailSink(object):
    """
    Alert sink to dispatch alerts by email.
    
    """
    
    
    EMAIL_HEADER = "From: %s\r\nTo: %s\r\n\r\n"
    
    
    def __init__(self, config):
        self.config = config
        server = getattr(self.config, "smtpServer", "localhost")
        self.smtp = smtplib.SMTP(server)
        # produces a lot of debug output, uncomment in case of email delivery issues
        # self.smtp.set_debuglevel(1)        
        login, passw = getattr(config, "smtpUser", None), getattr(config, "smtpPass", None)
        if login != None:
            self.smtp.login(login, passw)
        self.fromAddr = getattr(self.config, "fromAddr", None)
        self.toAddr  = getattr(self.config, "toAddr", None)
        logging.debug("%s initialized." % self.__class__.__name__)
        
        
    def send(self, alerts):
        """
        Handle list of alerts.
        
        """
        msg = self.EMAIL_HEADER % (self.fromAddr, ", ".join(self.toAddr))
        for a in alerts:
            msg += "\n%s\n" % a.toMsg()
        self.smtp.sendmail(self.fromAddr, self.toAddr, msg)
        logging.debug("%s sent alerts." % self.__class__.__name__)
        
        
    def __del__(self):
        if hasattr(self, "smtp"):
            self.smtp.quit()