import cherrypy
import re
import logging

class YUIServer:
    def __init__(self, config):
        self.yuidir = config.yuidir
        self.pattern = re.compile(r"^[-a-z_/]+\.(js|css|png)$")

    @cherrypy.expose
    def yui(self, *args):
        path = "/".join(args)
        if not self.pattern.match(path):
            logging.error("Bad YUI location "+path)
            raise cherrypy.HTTPError(403, "Bad YUI location")
        return cherrypy.lib.static.serve_file(self.yuidir + '/' + path)
