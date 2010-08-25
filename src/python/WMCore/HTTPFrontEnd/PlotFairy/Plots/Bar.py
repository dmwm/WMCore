from Mixins import *
from Validators import *
from Plot import Plot
import matplotlib.patches

class Bar(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,BinnedNumericSeriesMixin,LegendMixin,WatermarkMixin):
    '''
    Draw a bar chart with numeric axes and one or more series.
    Multiple series are stacked.
    
    TODO: Possibility of overlaying multiple series. Horizontal
    instead of vertical bars.
    '''
    __metaclass__=Plot
    def data(self):
        
        axes = self.figure.gca()
        
        if len(self.props.series)==0:
            return    
        
        self.props.legend_items = []
        
        nbins = self.props.xaxis['bins']
        edges = self.props.xaxis['edges']
        bottom = [self.props.series[0]['logmin']]*nbins
        left = edges[:-1]
        width = [edges[i+1]-edges[i] for i in range(nbins)]
        
        for series in self.props.series:
            height = series['values']
            colour = series['colour']
            label = series['label']
            bar = axes.bar(left,height,width,bottom,label=label,color=colour)
            bottom = [b+h for b,h in zip(bottom,height)]
            self.props.legend_items.append({'label':label,'artist':matplotlib.patches.Rectangle((0,0),1,1,facecolor=colour),'value':series['integral']})
            
        axes.set_ybound(lower=self.props.series[0]['logmin'])

        