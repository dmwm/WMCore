from Mixins import *
from Validators import *
from Plot import Plot


class LabelledBar(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XAutoLabelledAxisMixin,YNumericAxisMixin,LabelledSeriesMixin,WatermarkMixin,LegendMixin):
    '''
    Draw a bar chart with labelled bins.
    
    Uses the same syntax as a pie chart, so each bin can only
    contain a single data point.
    
    TODO: Multiple data points per bin, horizontal bars.
    '''
    __metaclass__=Plot
    def data(self):
        
        axes = self.figure.gca()
        
        series = self.props.series
        
        axes.set_xticks([i+0.5 for i in range(len(series))])
        
        labels = [item['label'] for item in series]
        left = range(len(series))
        bottom = [0]*len(series)
        width = [1]*len(series)
        height = [item['value'] for item in series]
        colour = [item['colour'] for item in series]
        
        axes.set_xticklabels(labels)
        axes.bar(left,height,width,bottom,label=labels,color=colour)