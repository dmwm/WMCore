from cherrypy import expose
from cherrypy import __version__ as cherrypy_version
from Cheetah import Version as cheetah_version
from WMCore.WebTools.Page import Page

class Welcome(Page):
    def __init__(self, namesAndDocstrings):
        self.namesAndDocstrings = namesAndDocstrings
 
    @expose
    def index(self):
        html = '<html>'
        html += '<head>'
        html += '<link rel="stylesheet" type="text/css" href="/controllers/css/reset/style"/>'
        html += '</head>'
        html += '<body>'
        html += '<h1>Welcome to the DMWM web framework</h1>'
        html += "<table>\n"
        html += "<tr><th><h3>Service</h3></th><th><h3>Description</h3></th></tr>\n"
        self.namesAndDocstrings.sort()
        for name, docstring in self.namesAndDocstrings:
            html += '<tr><td><p><a href="%s">%s</a></p></td><td><p>%s</p></td></tr>' % (name, name, docstring)
        html += "<tr><td><h3>Server info</h3</td></tr>\n"
        html += '<tr><td><p>CherryPy: v.%s</p>' % cherrypy_version
        html += '<p>Cheetah: v.%s</p></td></tr>' % cheetah_version
        html += '</table>'
        html += '</body>'
        html += '</html>'
        return html
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()
