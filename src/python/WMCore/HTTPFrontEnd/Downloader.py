from cherrypy.lib.static import serve_file
import os
import logging

class Downloader:
    """
    _Downloader_

    Serve files from the JobCreator Cache via HTTP

    """
    def __init__(self, config):
        print str(config)
        self.rootdir = config.dir

    def index(self, filepath):
        """
        _index_

        index response to download URL, serves the file
        requested

        """
        name = os.path.normpath(os.path.join(self.rootdir, filepath))
        logging.debug("Download Agent serving file: %s" % name)

        if os.path.commonprefix([name,  self.rootdir]) != self.rootdir:
            raise RuntimeError, "You tried to leave the CacheDir"
        if not os.path.exists(name):
            raise RuntimeError, "%s not found" % name
        return serve_file(name, "application/x-download", "attachment")

    index.exposed = True

