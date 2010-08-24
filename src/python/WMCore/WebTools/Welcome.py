from cherrypy import expose
from WMCore.WebTools.Page import Page

class Welcome(Page):
    def __init__(self, namesAndDocstrings):
        self.namesAndDocstrings = namesAndDocstrings
 
    @expose
    def index(self):
        html = '<h1>Welcome to the DMWM web framework</h1>'
        html += "<table>\n"
        html += "<tr><th>Service</th><th>Description</th></tr>\n"
        for name, docstring in self.namesAndDocstrings:
            html += '<tr><td><a href="%s">%s</a></td><td>%s</td></tr>' % (name, name, docstring)
        html += '</table>'
        return html
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()
