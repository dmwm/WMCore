'''
Builds a plot from json data.
'''
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.Services.Requests import JSONRequests
from WMCore.WMFactory import WMFactory

from cherrypy import request
import urllib
import re

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

URL_REGEX = re.compile("(ftp|http|https):\/\/(\w+:{0,1}\w*@)?([\w.]+)(:[0-9]+)?(\/([\w#!:.?+=&%@!\-\/~]+)|\/)?")
  
class Plotter(RESTModel):
    '''
    A class that generates a plot object to send to the formatter, possibly 
    downloading the data from a remote resource along the way
     
    '''
    def __init__(self, config):
        RESTModel.__init__(self, config)
        
        self.methods['POST'] = {'plot': {'version':1,
                                         'call':self.plot,
                                         'args': ['type', 'data', 'url'],
                                         'expires': 300}}
        self.methods['GET'] = {'plot': {'version':1,
                                         'call':self.plot,
                                         'args': ['type', 'data', 'url'],
                                         'expires': 300},
                               'doc':  {'version':1,
                                         'call':self.doc,
                                         'args': ['type'],
                                         'expires':300}}
        self.factory = WMFactory('plot_fairy',
                                 'WMCore.HTTPFrontEnd.PlotFairy.Plots')
        
    def plot(self, *args, **kwargs):
        input = self.sanitise_input(args, kwargs, 'plot')
            
        plot = self.factory.loadObject(input['type'])
        
        return {'figure': plot(input['data'])}
            
    def doc(self, *args, **kwargs):
        input = self.sanitise_input(args, kwargs, 'doc')
        plot = self.factory.loadObject(input['type'])
        return {'doc': plot.doc()}
            
    def validate_input(self, input, verb, method):
        assert 'type' in input.keys(), "no type provided - what kind of plot do you want?"
        if method=='doc':
            return input
        
        valid_data = {}
        assert 'data' in input.keys() or 'url' in input.keys(), "neither data nor url provided - please provide at least one"
        
        if 'url' in input.keys():
            match = URL_REGEX.match(input['url'])
            assert match != None, "`%s' is not a valid URL" % input['url'] 
            host = match.group(1) + '://' + match.group(3)
            if match.group(4):
                host += match.group(4)
            uri = match.group(5)
            result,status,reason,fromcache = JSONRequests(host).get(uri)
            valid_data.update(result)

        if 'data' in input.keys():
            valid_data.update(json.loads(urllib.unquote(input['data'])))
                 
        return {'data':valid_data,'type':input['type']}
    
