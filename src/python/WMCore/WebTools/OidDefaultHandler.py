#!/usr/bin/env python
from cherrypy import expose
import cherrypy
import urllib

DEFAULT_SESSION_NAME = 'SecurityModule'
DEFAULT_OID_SERVER = 'https://cmsweb.cern.ch/security/'

class OidDefaultHandler:
    def __init__(self, config):
        self.config = config
        self.session_name = getattr(self.config, 'session_name', DEFAULT_SESSION_NAME)
        self.oidserver = getattr(self.config, 'oid_server', DEFAULT_OID_SERVER)

    @expose
    def login(self, url='/'):
        redirect_url = "%s?%s" % (url, 
                              urllib.urlencode({'openid_url':self.oidserver}))
        raise cherrypy.HTTPRedirect(redirect_url)

    @expose
    def logout(self):
        if cherrypy.session.has_key(self.session_name):
            del cherrypy.session[self.session_name]
        return "Disconnected"

    @expose
    def failure(self):
        info = self.getSessionInfo()
        if info:
            return "Verification of %s failed." % info
        return "Verification failed for an unknown reason"

    @expose
    def cancelled(self):
        return "Verification cancelled: %s" % self.getSessionInfo()

    @expose
    def error(self):
        return "An error happened during the authentication: %s" \
               % self.getSessionInfo()

    @expose
    def authz(self):
        return "Authorization failed: %s"  % self.getSessionInfo()

    @expose
    def dummy(self):
        return self.getSessionInfo()

    def getSessionInfo(self):
        sessreg = cherrypy.session.get(self.session_name,None)
        if sessreg:
            return cherrypy.session[self.session_name].get('info',None)
        return None
