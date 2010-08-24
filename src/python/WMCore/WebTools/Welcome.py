from cherrypy import expose
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
        html += "<tr><th><p>Service</p></th><th><p>Description</p></th></tr>\n"
        for name, docstring in self.namesAndDocstrings:
            html += '<tr><td><p><a href="%s">%s</a></p></td><td><p>%s</p></td></tr>' % (name, name, docstring)
        html += '</table>'
        html += '</body>'
        html += '</html>'
        return html
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()
