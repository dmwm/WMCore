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
        login, passw = getattr(config, "smtpUser", None), getattr(config, "smtpPass", None)
        if login != None:
            self.smtp.login(login, passw)
        self.fromAddr = getattr(self.config, "fromAddr", None)
        self.toAddr  = getattr(self.config, "toAddr", None)
        
        
    def send(self, alerts):
        """
        Handle list of alerts.
        
        """
        msg = self.EMAIL_HEADER % (self.fromAddr, ", ".join(self.toAddr))
        for a in alerts:
            msg += "\n%s\n" % a.toMsg()
        self.smtp.sendmail(self.fromAddr, self.toAddr, msg)
        
        
    def __del__(self):
        self.smtp.quit()