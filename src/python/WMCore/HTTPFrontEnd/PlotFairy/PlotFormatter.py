#!/usr/bin/env python
'''
The plot fairy has a special formatter which generates pdf and png images.
Might also do some caching one day....
'''

from WMCore.WebTools.RESTFormatter import RESTFormatter

import matplotlib
matplotlib.use('Agg')
from cStringIO import StringIO

class PlotFormatter(RESTFormatter):
    def __init__(self, config):
        matplotlib.use('Agg')
        RESTFormatter.__init__(self, config)
 
        self.supporttypes = {'image/png': self.png,
                             '*/*': self.png,
                             'application/pdf': self.pdf,
                             'image/svg+xml':self.svg}
    
    def svg(self, figure):
        return self.plot(figure, 'svg')
    
    def pdf(self, figure):
        return self.plot(figure, 'pdf')

    def png(self, figure):
        return self.plot(figure, 'png')

    def plot(self, data, format):
        if hasattr(self.config, "cache"):
            # Write the figure to a file (use the tempfile module:
            # http://docs.python.org/library/tempfile.html) and return that file
            raise NotImplemented
        else:
            buffer = StringIO()
            data['figure'].savefig(buffer, data.get('dpi',96), format=format)
            return buffer.getvalue()