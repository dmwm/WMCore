from future.utils import viewitems

import cherrypy
from os import listdir

from cherrypy import expose, tools

from WMCore.WebTools.FrontEndAuth import get_user_info
from WMCore.WebTools.Page import TemplatedPage


class SecureDocumentation(TemplatedPage):
    """
    The documentation for the framework
    """

    @expose
    @tools.secmodv2()
    def index(self):
        templates = listdir(self.templatedir)
        user = get_user_info()
        index = "<h1>Secure Documentation</h1>"
        index += "<div>\n"
        index += "Hello <b>%s</b>!\n" % user['name']
        index += "<ul>\n"
        index += "<li>Your <b>DN</b>: %s</li>\n" % user['dn']
        index += "<li>Your <b>Login</b>: %s</li>\n" % user['login']
        index += "<li>Your <b>Authn Method</b>: %s</li>\n" % user['method']
        index += "<li>Your roles:\n"
        index += "<ul>\n"
        for k, v in viewitems(user['roles']):
            for t in ['group', 'site']:
                for n in v[t]:
                    index += "<li><b>%s</b>: %s=%s</li>\n" % (k, t, n)
        index += "</ul></li></ul>\n"
        index += "<a href='CernDocs'>CERN docs</a>\n"
        index += "<a href='UerjDocs'>UERJ docs</a>\n"
        index += "</div>\n"

        index += "<ol>"
        for t in templates:
            if '.tmpl' in t:
                index = "%s\n<li><a href='%s'>%s</a></li>" % (index,
                                                              t.replace('.tmpl', ''),
                                                              t.replace('.tmpl', ''))
        index = "%s\n<li><a href='https://twiki.cern.ch/twiki/bin/view/CMS/DMWebtools'>twiki</a>" % (index)
        index = "%s\n</ol>" % (index)

        return index

    @expose
    @tools.secmodv2(role='admin')
    def AdminDocs(self):
        return "This is a secret documentation for admins."

    @expose
    @tools.secmodv2(site='t1-ch-cern')
    def CernDocs(self):
        return "This is a secret documentation about the T1 CERN site"

    @expose
    @tools.secmodv2(role='dev', group='dmwm')
    def DevDmwmDocs(self):
        return "This is a secret documentation for DMWM developers"

    @expose
    def default(self, *args, **kwargs):
        if len(args) > 0:
            return self.templatepage(args[0])
        return self.index()
