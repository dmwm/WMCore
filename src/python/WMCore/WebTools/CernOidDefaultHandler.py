#!/usr/bin/env python
from cherrypy import expose
import cherrypy

DEFAULT_SESSION_NAME = 'CernOpenIdTool'

class CernOidDefaultHandler:
    def __init__(self, config):
        print '>>>>>> CernOidDefaultHandler: %s' % config
        self.config = config
        self.session_name = self.config.session_name

    @expose
    def login(self, url='/'):
        return """\
<html>
  <head />
  <body>
    <p>Enter your OpenID:</p>
    <form method="get" action="%s">
      <input type="text" name="openid_url" value="" />
      <input type="submit" />
    </form>
  </body>
</html>
""" % (url)

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

    def getSessionInfo(self):
        sessreg = cherrypy.session.get(self.session_name,None)
        if sessreg:
            return cherrypy.session[self.session_name].get('info',None)
        return None
