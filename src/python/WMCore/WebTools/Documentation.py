from cherrypy import expose
from Page import TemplatedPage
from os import listdir
class Documentation(TemplatedPage):
    @expose
    def index(self):
        templates = listdir(self.templatedir)
        index = "<h1>Documentation</h1>\n<ol>"
        for t in templates:
            index = "%s\n<li><a href='%s'>%s</a></li>" % (index, 
                                                      t.replace('.tmpl', ''), 
                                                      t.replace('.tmpl', ''))
        index = "%s\n<ol>" % (index)
        return index
    
    @expose
    def default(self, *args, **kwargs):
        if len(args) > 0:
            return self.templatepage(args[0])
        else:
            return self.index()