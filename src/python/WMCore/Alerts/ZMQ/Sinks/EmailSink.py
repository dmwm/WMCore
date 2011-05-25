#!/usr/bin/env python
# encoding: utf-8
"""
EmailSink.py

Created by Dave Evans on 2011-04-27.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import smtplib


class EmailSink:
    """
    _EmailSink_
    
    Alert sink to dispatch alerts by email
    """        
    def __init__(self, config):
        self.config = config
        self.smtp = smtplib.SMTP(getattr(self.config, 'smtpServer', 'localhost'))
        login, passw = getattr(config, 'smtpUser', None), getattr(config, 'smtpPass', None)
        if login != None:
            self.smtp.login(login, passw)
        self.fromAddr = getattr(self.config, "fromAddr")
        self.toAddrs  = getattr(self.config, "toAddr")

        
    def send(self, alerts):
        """
        _send_
        
        handle list of alerts
        """
        msg = "From: %s\r\nTo: %s\r\n\r\n" % (self.fromAddr, ", ".join(self.toAddrs))
        for a in alerts:
            msg += "\n%s\n" % str(a)
        self.smtp.sendmail(self.fromAddr, self.toAddrs, msg)
        
    def __del__(self):
        self.smtp.quit()


