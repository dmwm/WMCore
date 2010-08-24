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
    
    def svg(self, data, *args, **kwargs):
        return self.plot(data, 'svg')
    
    def pdf(self, data, *args, **kwargs):
        return self.plot(data, 'pdf')

    def png(self, data, *args, **kwargs):
        if 'figure' in data:
            return self.plot(data, 'png')
        elif 'doc' in data:
            return data['doc']
        else:
            return None
            

    def plot(self, data, format):
        if hasattr(self.config, "cache"):
            # Write the figure to a file (use the tempfile module:
            # http://docs.python.org/library/tempfile.html) and return that file
            raise NotImplemented
        else:
            if 'figure' in data:
                buffer = StringIO()
                data['figure'].savefig(buffer, format=format)
                return buffer.getvalue()
            return None
    