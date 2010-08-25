from Plot import Plot
from Utils import *

class Cumulative(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,NumericSeriesMixin):
    __metaclass__=Plot
    def data(self):
        
        axes = self.figure.gca()
        nbins = self.props.xaxis['bins']
        edges = self.props.xaxis['edges']
        
        bottom = [0]*nbins
        left = edges[:-1]
        width = [edges[i+1]-edges[i] for i in range(nbins)]
        
        for series in self.props.series:
            top = series['values']
            
            colour = series['colour']
            label = series['label']
            
            top = [t+b for t,b in zip(top,bottom)]
            
            axes.fill_between(left,bottom,top,label=label,facecolor=colour)
            bottom = top
        
        axes.set_ybound(0)