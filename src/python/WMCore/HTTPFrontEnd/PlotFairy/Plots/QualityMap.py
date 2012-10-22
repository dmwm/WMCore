from Plot import Plot
from Mixins import *
from Validators import *

class QualityMap(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XAnyBinnedAxisMixin,YAnyBinnedAxisMixin,ArrayMixin,WatermarkMixin):
    '''
    Draws a coloured quality map based on an array of 0->1 datapoints,
    a la the phedex link quality plot.

    Values between 0 and 1 are interpolated between colour0 and colour1.

    The array needs to have the same dimensions as the number of bins
    specified on both the x and y axes. Each can be either a specified
    with 3 of (min, max, width, bins), or a list of labels.

    TODO: Currently has no support for blanking cells.
    '''
    __metaclass__=Plot
    def __init__(self):
        self.validators = [ColourBase('colour0',default='#ff0000',doc_user="Colour for values ->0."),
                           ColourBase('colour1',default='#00ff00',doc_user="Colour for values ->1.")]
        self.props = Props()
        super(QualityMap,self).__init__(Array_Min=0.,Array_Max=1.)
    def data(self):
        axes = self.figure.gca()

        if self.props.xaxis['bins']==None or self.props.yaxis['bins']==None:
            return

        x_edges = self.props.xaxis['edges']
        y_edges = self.props.yaxis['edges']
        x_bins = self.props.xaxis['bins']
        y_bins = self.props.yaxis['bins']

        left = x_edges[:-1]
        bottom = y_edges[:-1]
        width = [x_edges[i+1]-x_edges[i] for i in range(x_bins)]
        height = [y_edges[i+1]-y_edges[i] for i in range(y_bins)]

        c0 = self.props.colour0
        c1 = self.props.colour1

        val2colour = lambda x: [c0[i] + x*(c1[i]-c0[i]) for i in (0,1,2)]

        for y,row in enumerate(self.props.data):
            for x,item in enumerate(row):
                axes.bar(left[x],height[y],width[x],bottom[y],facecolor=val2colour(item))
