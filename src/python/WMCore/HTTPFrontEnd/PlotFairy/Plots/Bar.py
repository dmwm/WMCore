#from matplotlib.pyplot import figure
#import matplotlib.cm as cm
#from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot
import math

import Utils

class NumericBar(Plot):
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
            height = series['values']
            if self.props['yaxis']['log']:
                cls = Utils.CleanLogSeries(height)
                height = cls.remove_negative()
            
            colour = series['colour']
            label = series['label']
            axes.bar(left,height,width,bottom,label=label,color=colour)
            bottom = [b+h for b,h in zip(bottom,height)]
        
        return figure

class LabelledBar(Plot):
    def __init__(self):
        self.props = {}
        self.mixins = [Utils.FigureMixin(self.props),Utils.TitleMixin(self.props),Utils.FigAxesMixin(self.props),Utils.StyleMixin(self.props),Utils.AutoLabelledAxisMixin(self.props,'xaxis'),Utils.NumericAxisMixin(self.props,'yaxis'),Utils.LabelledSeriesMixin(self.props)]
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
        
        series = self.props['series']
        
        axes = figure.gca()
        axes.set_xticks([i+0.5 for i in range(len(series))])
        
        logmin = 0
        if self.props['yaxis']['log']:
            cls = Utils.CleanLogSeries([item['value'] for item in series])
            if cls.minpos:
                logmin = cls.roundmin()
            else:
                return figure
        
        labels = [item['label'] for item in series]
        left = range(len(series))
        bottom = [logmin]*len(series)
        width = [1]*len(series)
        height = [item['value'] for item in series]
        colour = [item['colour'] for item in series]
        
        axes.set_xticklabels(labels)
        axes.bar(left,height,width,bottom,label=labels,color=colour)
        
        return figure
        