#!/usr/bin/env python
from cherrypy import expose, session

DEFAULT_SESSION_NAME = 'CernOpenIdTool'

class CernOidDefaultHandler:
    def __init__(self, session_name=DEFAULT_SESSION_NAME):
        self.session_name=session_name

    @expose
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

    @expose
    def logout(self):
        if session.has_key(self.session_name):
            del session[self.session_name]
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
        sessreg = session.get(self.session_name,None)
        if sessreg:
            return session[self.session_name].get('info',None)
        return None
