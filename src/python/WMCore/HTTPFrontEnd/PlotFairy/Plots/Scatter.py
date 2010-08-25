from Plot import Plot
from Mixins import *
from Validators import *

class Scatter(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XAutoLabelledAxisMixin,YAutoLabelledAxisMixin,LabelledSeries2DMixin,WatermarkMixin):
    __metaclass__=Plot
    def __init__(self):
        self.props = Props()
        self.validators = [ElementBase('draw_lines',bool,default=False)]
        super(Scatter,self).__init__()
    def data(self):
        axes = self.figure.gca()
        
        for series in self.props.series:
            axes.plot(series['x'],series['y'],markerfacecolor=series['colour'],marker=series['marker'],label=series['label'],linestyle='-' if self.props.draw_lines else '')