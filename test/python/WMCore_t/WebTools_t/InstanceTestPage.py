#!/usr/bin/env python
# encoding: utf-8
"""
InstanceTestPage.py

"""
from cherrypy import expose
from WMCore.WebTools.Page import Page

class InstanceTestPage(Page):
    @expose
    def index(self):
        return self.config.instance

    @expose
    def default(self, *args, **kwargs):
        return self.index()

    @expose
    def database(self):
        return self.config.database.connectUrl

    @expose
    def security(self):
        return self.config.security.sec_params
