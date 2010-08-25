from Plot import Plot
from Mixins import *
from Validators import *

class Scatter(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XAutoLabelledAxisMixin,YAutoLabelledAxisMixin,LabelledSeries2DMixin,WatermarkMixin):
    '''
    Draws an x-y scatter diagram, possibly with multiple data series.
    
    Series for this look like 1d series but have lists 'x' and 'y' instead
    of one list 'values'.
    
    TODO: Legend, per-series control over line drawing, error bars,
    line interpolation instead of point-to-point drawing as an option.
    
    '''
    __metaclass__=Plot
    def __init__(self):
        self.props = Props()
        self.validators = [ElementBase('draw_lines',bool,default=False,doc_user="Connect points with lines.")]
        super(Scatter,self).__init__()
    def data(self):
        axes = self.figure.gca()
        
        for series in self.props.series:
            axes.plot(series['x'],series['y'],markerfacecolor=series['colour'],marker=series['marker'],label=series['label'],linestyle='-' if self.props.draw_lines else '')