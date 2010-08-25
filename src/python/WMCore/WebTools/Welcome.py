import time
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
        html += '<link rel="stylesheet" type="text/css" href="/controllers/css/reset/style.css"/>'
        html += '</head>'
        html += '<body>'
        html += '<h1>Welcome to the DMWM web framework</h1>'
        html += '<hr style="width:100%;border-top: 1px dotted #CCCCCC;" />'
        html += "<table>\n"
        html += '<tr><th align="left"><h3>Service</h3></th>\n'
        html += '<th align="left"><h3>Description</h3></th></tr>\n'
        self.namesAndDocstrings.sort()
        for name, docstring in self.namesAndDocstrings:
            html += '<tr><td><p><a href="%s">%s</a></p></td>\n' \
                % (name, name)
            html += '<td><p>%s</p></td></tr>\n' % docstring
        html += '</table><br />'
        html += '<hr style="width:100%;border-top: 1px dotted #CCCCCC;" />'
        html += '<div style="font-size: 12px;font-weight: normal;font-family: helvetica;">'
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        html += '<b>Server info:</b> CherryPy v%s, Cheetah: v%s, %s' \
                % (cherrypy_version, cheetah_version, timestamp)
        html += '</div>'
        html += '</body>'
        html += '</html>'
        return html
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()
