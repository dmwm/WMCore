from Plot import Plot
from Mixins import *
from Validators import *

class Wave(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,BinnedNumericSeriesMixin):
    __metaclass__=Plot
    def data(self):
        
        axes = self.figure.gca()
        axes.set_axis_off()
        nbins = self.props.xaxis['bins']
        edges = self.props.xaxis['edges']
        
        if len(self.props.series)==0:
            return
        
        left = edges[:-1]
        
        totals = [sum([s['values'][i] for s in self.props.series]) for i in range(nbins)]
        maxval = max(totals)
        
        bottom = [0.5*(maxval-t) for t in totals]
        
        for series in self.props.series:
            top = series['values']
            
            colour = series['colour']
            label = series['label']
            
            top = [t+b for t,b in zip(top,bottom)]
            
            axes.fill_between(left,bottom,top,label=label,facecolor=colour)
            bottom = top
        
        #axes.set_xbound(edges[0],edges[-1])
        