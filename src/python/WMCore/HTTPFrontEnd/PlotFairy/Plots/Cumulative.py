from matplotlib.pyplot import figure
from matplotlib.patches import Rectangle
import numpy as np
import math
from Plot import Plot

import Utils

class Cumulative(Plot):
    def __init__(self):
        self.props = {}
        self.mixins = [Utils.FigureMixin(self.props),Utils.TitleMixin(self.props),Utils.FigAxesMixin(self.props),Utils.StyleMixin(self.props),Utils.BinnedNumericAxisMixin(self.props,'xaxis'),Utils.NumericAxisMixin(self.props,'yaxis'),Utils.NumericSeriesMixin(self.props)]
    def validate_input(self,input):
        for mixin in self.mixins:
            res = mixin.validate(input)
            if res==True:
                continue
            else:
                return res
        return True
    def plot(self,input):
        figure = None
        for mixin in self.mixins:
            figure = mixin.apply(figure)
        
        axes = figure.gca()
        
        nbins = self.props['xaxis']['_bins']
        edges = self.props['xaxis']['_edges']
        
        logmin = 0
        if self.props['yaxis']['log']:
            cls = Utils.CleanLogSeries(self.props['series'][0]['values'])
            if cls.minpos:
                logmin = cls.roundmin()
            else:
                return figure
        
        bottom = [logmin]*nbins
        left = edges[:-1]
        width = [edges[i+1]-edges[i] for i in range(nbins)]
        
        
        for series in self.props['series']:
            top = series['values']
            if self.props['yaxis']['log']:
                cls = Utils.CleanLogSeries(top)
                top = cls.remove_negative()
            
            colour = series['colour']
            label = series['label']
            
            top = [t+b for t,b in zip(top,bottom)]
            
            axes.fill_between(left,bottom,top,label=label,facecolor=colour)
            bottom = top
        
        axes.set_ybound(logmin)
        
        return figure

"""
     if input.get('legend',False):
            axes.legend([Rectangle((0,0),1,1,fc=s['colour']) for s in series],[s['label'] for s in series],loc=0)
        return fig
"""