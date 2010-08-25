#!/usr/bin/env python
import cherrypy

class CernOidDefaultHandler:
    def __init__(self,session_name=DEFAULT_SESSION_NAME):
        self.session_name=session_name

    @cherrypy.expose
    def login(self):
        return """\
<html>
  <head />
  <body>
    <p>Enter your OpenID:</p>
    <form method="get" action="/">
      <input type="text" name="openid_url" value="" />
      <input type="submit" />
    </form>
  </body>
</html>
"""

    @cherrypy.expose
    def logout(self):
        if cherrypy.session.has_key(self.session_name):
            del cherrypy.session[self.session_name]
        return "Disconnected"

    @cherrypy.expose
    def failure(self):
        info = self.getSessionInfo()
        if info:            
            return "Verification of %s failed." % info
        return "Verification failed for an unknown reason"

    @cherrypy.expose
    def cancelled(self):
        return "Verification cancelled: %s" % self.getSessionInfo()

    @cherrypy.expose
    def error(self):
        return "An error happened during the authentication: %s" \
               % self.getSessionInfo()

    def getSessionInfo(self):
        sessreg = cherrypy.session.get(self.session_name,None)
        if sessreg:
            return cherrypy.session[self.session_name].get('info',None)
        return None
