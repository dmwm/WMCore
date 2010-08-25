#!/usr/bin/env python
'''
The plot fairy has a special formatter which generates pdf and png images.
Might also do some caching one day....
'''

from WMCore.WebTools.RESTFormatter import RESTFormatter

import matplotlib
from StringIO import StringIO

class PlotFormatter(RESTFormatter):
    def __init__(self, config):
        matplotlib.use('Agg')
        RESTFormatter.__init__(self, config)
 
        self.supporttypes.update({'image/png': self.png,
                                  '*/*': self.png,
                                  'application/pdf': self.pdf})
    
    def pdf(self, data):
        return self.plot(figure, 'pdf')

    def png(self, figure):
        return self.plot(figure, 'png')

    def plot(self, figure, format):
        if hasattr(self.config, "cache"):
            # Write the figure to a file (use the tempfile module:
            # http://docs.python.org/library/tempfile.html) and return that file
            pass
        else:
            buffer = StringIO()
            figure.savefig(buffer, dpi = 300, format=format)
            return buffer.getvalue()