#!/usr/bin/env python

"""
The documentation for the framework
"""




from cherrypy import expose
from WMCore.WebTools.Page import TemplatedPage
from os import listdir

class Documentation(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    def index(self):
        """
        The index of the documentation
        """
        templates = listdir(self.templatedir)
        index = "<h1>Documentation</h1>\n<ol>"
        for t in templates:
            if '.tmpl' in t:
                index = "%s\n<li><a href='%s'>%s</a></li>" % (index,
                                                      t.replace('.tmpl', ''),
                                                      t.replace('.tmpl', ''))
        index = "%s\n<li><a href='https://twiki.cern.ch/twiki/bin/view/CMS/DMWebtools'>twiki</a>" % (index)
        index = "%s\n<ol>" % (index)
        return index

    @expose
    def default(self, *args, **kwargs):
        """
        Show the documentation for a page or return the index
        """
        if len(args) > 0:
            return self.templatepage(args[0], config=self.config)
        else:
            return self.index()
