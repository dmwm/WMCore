from Plot import Plot
from Utils import *

class Cumulative(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,BinnedNumericSeriesMixin):
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
        
        for series in self.props.series:
            top = series['values']
            
            colour = series['colour']
            label = series['label']
            
            top = [t+b for t,b in zip(top,bottom)]
            
            axes.fill_between(left,bottom,top,label=label,facecolor=colour)
            bottom = top
        
        axes.set_xbound(edges[0],edges[-1])
        axes.set_ybound(logmin)
        