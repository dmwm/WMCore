from Utils import *
from Plot import Plot

class Bar(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,NumericSeriesMixin):
    __metaclass__=Plot
    def __init__(self):
        super(Bar,self).__init__()
    def data(self):
        
        axes = self.figure.gca()
        
        nbins = self.props.xaxis['bins']
        edges = self.props.xaxis['edges']
        bottom = [0]*nbins
        left = edges[:-1]
        width = [edges[i+1]-edges[i] for i in range(nbins)]
        
        print nbins,edges
        
        for series in self.props.series:
            height = series['values']
            colour = series['colour']
            label = series['label']
            axes.bar(left,height,width,bottom,label=label,color=colour)
            bottom = [b+h for b,h in zip(bottom,height)]
    
class LabelledBar(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XAutoLabelledAxisMixin,YNumericAxisMixin,LabelledSeriesMixin):
    __metaclass__=Plot
    def __init__(self):
        super(LabelledBar,self).__init__()
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
        