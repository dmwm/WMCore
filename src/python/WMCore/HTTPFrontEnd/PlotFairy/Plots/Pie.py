from Plot import Plot
from Utils import *

class Pie(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,LabelledSeriesMixin):
    __metaclass__=Plot
    def data(self):
        axes = self.figure.gca()
        series = self.props.series
        
        labels = [item['label'] for item in series]
        value = [item['value'] for item in series]
        colour = [item['colour'] for item in series]
        explode = [item['explode'] for item in series]
        
        axes.pie(value, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True, colors=colour)
