from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot

class Scatter(Plot):
    def plot(self, input):
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
        xy = (input['width']/input.get('dpi',96), input['height']/input.get('dpi',96))
        fig = figure(figsize=xy, dpi=input.get('dpi',96))
        axis = fig.add_axes([0.2, 0.1, 0.75, 0.75], 
                            xlabel=input['x-name'], 
                            ylabel=input['y-name'],
                            title=input.get('title',
                                                    input.get('x-name',
                                                                  'My Plot')))
        point_styles = ['rs', 'go', 'bt', 'y*']
        lines = ()
        titles = ()
        for series in input['series']:
            x = series[input['x-name']]
            y = series[input['y-name']]
            l = axis.plot(x, y, point_styles[input['series'].index(series)])
            lines += (l,)
            titles += (series['title'],)
        fig.legend(lines, titles, 'upper right')

        return fig