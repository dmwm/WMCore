#!/usr/bin/env python
"""
A sane default welcome page
"""





import time
from cherrypy import expose
from cherrypy import __version__ as cherrypy_version
from Cheetah import Version as cheetah_version
from WMCore.WebTools.Page import Page

class Welcome(Page):
    """
    A sane default welcome page
    """
    def __init__(self, namesAndDocstrings):
        Page.__init__(self, {})
        self.namesAndDocstrings = namesAndDocstrings
 
    @expose
    def index(self):
        """
        Return an index page of all the pages in the web app.
        """
        html = '<html>'
        html += '<head>'
        html += '<link rel="stylesheet" type="text/css" '
        html += 'href="/controllers/css/reset/style.css"/>'
        html += '</head>'
        html += '<body>'
        html += '<h1>Welcome to the DMWM web framework</h1>'
        html += '<hr style="width:100%;border-top: 1px dotted #CCCCCC;" />'
        html += "<table>\n"
        html += '<tr><th align="left"><h3>Service</h3></th>\n'
        html += '<th align="left"><h3>Description</h3></th></tr>\n'
        self.namesAndDocstrings.sort()
        for name, docstring in self.namesAndDocstrings:
            html += '<tr><td><p><a href="/%s">%s</a></p></td>\n' \
                % (name, name)
            html += '<td><p>%s</p></td></tr>\n' % docstring
        html += '</table><br />'
        html += '<hr style="width:100%;border-top: 1px dotted #CCCCCC;" />'
        html += '<div style="font-size: 12px; '
        html += 'font-weight: normal;font-family: helvetica;">'
        timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        html += '<b>Server info:</b> CherryPy v%s, Cheetah: v%s, %s' \
                % (cherrypy_version, cheetah_version, timestamp)
        html += '</div>'
        html += '</body>'
        html += '</html>'
        return html
    
    @expose
    def default(self, *args, **kwargs):
        """
        Return the index.
        """
        return self.index()
