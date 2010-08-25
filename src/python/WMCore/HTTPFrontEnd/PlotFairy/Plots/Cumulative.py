from Plot import Plot
from Mixins import *
from Validators import *

import matplotlib.patches

class Cumulative(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,BinnedNumericSeriesMixin,WatermarkMixin,LegendMixin):
    '''
    Draws a cumulative plot of one or more series.
    
    The 'values' arrays in the series for this plot should
    contain one more data point than the number of bins, otherwise
    the last point will be interpolated down to zero.
    '''
    __metaclass__=Plot
    def __init__(self):
        self.validators = []
        self.props = Props()
        super(Cumulative,self).__init__(BinnedNumericSeries_DataMode='edge')
    def data(self):
        
        axes = self.figure.gca()
        nbins = self.props.xaxis['bins']
        edges = self.props.xaxis['edges']
        
        if len(self.props.series)==0:
            return
        
        logmin = self.props.series[0]['logmin']
        bottom = [logmin]*(nbins+1)
        left = edges
        
        self.props.legend_items = []
        
        for series in self.props.series:
            top = series['values']
            
            colour = series['colour']
            label = series['label']
            
            top = [t+b for t,b in zip(top,bottom)]
            
            axes.fill_between(left,bottom,top,label=label,facecolor=colour)
            self.props.legend_items.append({'label':label,'artist':matplotlib.patches.Rectangle((0,0),1,1,facecolor=colour),'value':series['integral']})
            bottom = top
        
        axes.set_xbound(edges[0],edges[-1])
        axes.set_ybound(lower=logmin)
        