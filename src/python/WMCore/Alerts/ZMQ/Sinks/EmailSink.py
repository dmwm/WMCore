#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-04-27.
Copyright (c) 2011 Fermilab. All rights reserved.

"""

import smtplib



class EmailSink(object):
    """
    Alert sink to dispatch alerts by email.
    
    """
    
            
    def __init__(self, config):
        self.config = config
        server = getattr(self.config, "smtpServer", "localhost")
        self.smtp = smtplib.SMTP(server)
        login, passw = getattr(config, "smtpUser", None), getattr(config, "smtpPass", None)
        if login != None:
            self.smtp.login(login, passw)
        self.fromAddr = getattr(self.config, "fromAddr", None)
        self.toAddrs  = getattr(self.config, "toAddr", None)
        
        
    def send(self, alerts):
        """
        Handle list of alerts.
        
        """
        msg = "From: %s\r\nTo: %s\r\n\r\n" % (self.fromAddr, ", ".join(self.toAddrs))
        for a in alerts:
            msg += "\n%s\n" % str(a)
        self.smtp.sendmail(self.fromAddr, self.toAddrs, msg)
        
        
    def __del__(self):
        self.smtp.quit()