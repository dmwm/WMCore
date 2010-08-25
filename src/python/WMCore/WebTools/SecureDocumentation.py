from cherrypy import expose, tools
import cherrypy
from WMCore.WebTools.Page import TemplatedPage
from os import listdir

class SecureDocumentation(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    @tools.cernoid()
    def index(self):
        templates = listdir(self.templatedir)
        index = "<h1>Secure Documentation</h1>"
        index += "You are logged in using <a href='%s'>%s</a>\n" % \
                (cherrypy.session['SecurityModule']['openid_url'],
                 cherrypy.session['SecurityModule']['openid_url'])
        index += "Your role is: %s\n" % cherrypy.session['SecurityModule']['role']
        index += "Your group is: %s\n" % cherrypy.session['SecurityModule']['group']
        index += "Your site is: %s\n" % cherrypy.session['SecurityModule']['site']
        index += "\n<ol>"
        for t in templates:
            if '.tmpl' in t:
                index = "%s\n<li><a href='%s'>%s</a></li>" % (index, 
                                                      t.replace('.tmpl', ''), 
                                                      t.replace('.tmpl', ''))
        index = "%s\n<li><a href='https://twiki.cern.ch/twiki/bin/view/CMS/DMWebtools'>twiki</a>" % (index)
        index = "%s\n<ol>" % (index)
        return index

    @expose
    @tools.cernoid(role='Admin',group='CMS',site='T2_BR_UERJ')
    def UerjDocs(self):
        return "This is a secret documentation about the T2 UERJ site"

    @expose
    @tools.cernoid(role='Admin',group='CMS',site='T1_CH_CERN')
    def CernDocs(self):
        return "This is a secret documentation about the T1 CERN site"

    @expose
    def default(self, *args, **kwargs):
        if len(args) > 0:
            return self.templatepage(args[0])
        else:
            return self.index()
