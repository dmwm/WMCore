from cherrypy import expose
from WMCore.WebTools.Page import Page

class Welcome(Page):
    @expose
    def index(self):
        return """
<h1>Welcome to the DMWM web framework</h1>
<p><a href="documentation">Documentation</a></p>
        """
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()