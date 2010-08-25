'''
Builds a plot form json data.
'''
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.Services.Requests import JSONRequests
from matplotlib.pyplot import figure
import numpy as np
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
    
class Plotter(RESTModel):
    '''
    A class that generates a plot object to send to the formatter, possibly 
    downloading the data from a remote resource along the way
     
    '''

    def __init__(self, config):
        RESTModel.__init__(self, config)
        
        self.methods['POST'] = {'plot': {'version':1,
                                         'call':self.plot,
                                         'args': ['type', 'data', 'url']}}
        self.methods['GET'] = {'plot': {'version':1,
                                         'call':self.plot,
                                         'args': ['type', 'data', 'url']}}
        self.plot_types = {'bar': self.bar, 
                           'pie':self.pie, 
                           'scatter':self.scatter}
    
    def bar(self, input):
        """
        Produce an object representing a bar chart
        """
        pass
    
    def pie(self, input):
        """
        Produce an object representing a pie chart
                
        {'data': {'series':{'values': [15,30,45, 10],
                  'labels':['Frogs', 'Hogs', 'Dogs', 'Logs'],
                  'explode':2}, # the slice to pull out , if not set don't explode
            'height': 400,
            'width': 400,  
            'title': 'My plot title'}, # optional, fall back to the value of x-name
        'type': 'pie'}
        """
        
        xy = (input['data']['width']/96, input['data']['height']/96)
        fig = figure(figsize = xy)
        axis = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        
        labels = input['data']['series']['labels']
        fracs = input['data']['series']['values']
        if 'explode' in input['data']['series'].keys():
            ind = input['data']['series']['explode'] - 1
            explode = [0] * len(fracs)
            explode[ind] = 0.05
        axis.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True)
        
        return fig
    
    def scatter(self, input):
        """
        Produce an object representing a scatter chart
        Expects a json document like:
        
        {'data': { 
                'series': [
                    {'title': u'Series 1', 
                    'Std': [2, 3, 4, 1, 2], 
                    'Means': [20, 35, 30, 35, 27]}, 
                    {'title': u'Series 2', 
                    'Std': [3, 5, 2, 3, 3], 
                    'Means': [25, 32, 34, 20, 25]}
                ], # each series will be plotted
            'height': 400,
            'width': 400, 
            'x-name': 'Means', # what to plot on the x axis from the dicts in "series"
            'y-name': 'Std', # what to plot on the y axis from the dicts in "series" 
            'title': 'My plot title', # optional, fall back to the value of x-name
            'error': 'Std', # or x-error & y-error, values to plot as error bars - optional
            }
        'type': 'scatter'}
        
        returns a matplotlib Figure object
        """
        xy = (input['data']['width']/96, input['data']['height']/96)
        fig = figure(figsize = xy)
        axis = fig.add_axes([0.2, 0.1, 0.75, 0.75], 
                            xlabel=input['data']['x-name'], 
                            ylabel=input['data']['y-name'],
                            title=input['data'].get('title',
                                                    input['data'].get('x-name',
                                                                  'My Plot')))
        point_styles = ['rs', 'go', 'bt', 'y*']
        lines = ()
        titles = ()
        for series in input['data']['series']:
            x = series[input['data']['x-name']]
            y = series[input['data']['y-name']]
            l = axis.plot(x, y, point_styles[input['data']['series'].index(series)])
            lines += (l,)
            titles += (series['title'],)
        fig.legend(lines, titles, 'upper right')

        return fig
    
    def plot(self, args, kwargs):
        input = self.sanitise_input(args, kwargs)
        if not input['data']:
            # We have a URL for some json - we hope!
            jr = JSONRequests(input['url'])
            input['data'] = jr.get()
        
        return self.plot_types[input['type']](input)
            
    def validate_input(self, input, verb, method):
        if not 'data' in input.keys():
            input['data'] = {'height': 600, 'width': 800}
            assert 'url' in input.keys()
            reg = "(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?"
            assert re.compile(reg).match(input['url']) != None , \
              "'%s' is not a valid URL (regexp: %s)" % (input['url'], reg)
        else:
            input['data'] = json.loads(input['data'])
            
        if not 'height' in input['data'].keys():
            input['data']['height'] = 600
        else:
            input['data']['height'] = int(input['data']['height'])
            
        if not 'width' in input['data'].keys():
            input['data']['width'] = 800
        else:
            input['data']['width'] = int(input['data']['width'])
            
        assert 'type' in input.keys(), \
                "no type provided - what kind of plot do you want? " +\
                "Choose one of %s" % self.plot_types.keys()
        assert input['type'] in self.plot_types.keys(), \
                "the plot you chose (%s) is not in the supported types (%s)" %\
                        (input['type'],self.plot_types.keys()) 
        return input