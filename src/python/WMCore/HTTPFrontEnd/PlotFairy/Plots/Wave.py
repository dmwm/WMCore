from Plot import Plot
from Mixins import *
from Validators import *

class Wave(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,XBinnedNumericAxisMixin,YNumericAxisMixin,BinnedNumericSeriesMixin,WatermarkMixin):
    __metaclass__=Plot
    def __init__(self):
        self.props = Props()
        self.validators = [IntBase('text_size_min',min=1,default=4),
                           IntBase('truncate_text',default=None),
                           ElementBase('labelled',bool,default=True),
                           FloatBase('text_span_bins',default=8.)]
        super(Wave,self).__init__()
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
            if self.props.labelled:
                midpoint_height = [0.5*(top[i]+top[i+1]) for i in range(len(top)-1)]
                midpoint_best = midpoint_height.index(max(midpoint_height))
            
                text_centre_x = 0.5*(left[midpoint_best+1]+left[midpoint_best])
                text_centre_y = 0.5*(bottom[midpoint_best+1]+bottom[midpoint_best])+0.5*midpoint_height[midpoint_best]
                
                series['wave_centre']=(text_centre_x,text_centre_y)
                
                series['wave_bbox']=(
                                     (left[midpoint_best],bottom[midpoint_best]),
                                     (left[midpoint_best],bottom[midpoint_best]+top[midpoint_best]),
                                     (left[midpoint_best+1],bottom[midpoint_best+1]),
                                     (left[midpoint_best+1],bottom[midpoint_best+1]+top[midpoint_best+1])
                                     )
                #axes.arrow(text_centre_x,text_centre_y,1,1)                
            colour = series['colour']
            label = series['label']
            
            top = [t+b for t,b in zip(top,bottom)]
            
            
            
            axes.fill_between(left,bottom,top,label=label,facecolor=colour)
            bottom = top
        
        if self.props.labelled:
            for series in self.props.series:
                bbox = series['wave_bbox'][:]
                centre = series['wave_centre'][:]
                bbox = [axes.transData.transform_point(b) for b in bbox]
                centre = axes.transData.transform_point(centre)
                dx = 0.5*(bbox[2][0]-bbox[0][0])
                dy = centre[1] - 0.5*(bbox[3][1]+bbox[2][1])
                
                text_rotation = math.degrees(math.atan2(-dy,dx))
            
                cy = 0.5*(0.5*(bbox[1][1]-bbox[0][1])+0.5*(bbox[3][1]-bbox[2][1]))
                cx = dx*2*self.props.text_span_bins
                
                text = series['label']
                if self.props.truncate_text and len(text)>=self.props.truncate_text:
                    text = text[:self.props.truncate_text-1]+'..'
             
                text_size=min(font_size(text,cx),font_size('',cy))
                
                if text_size>=self.props.text_size_min:
                    axes.text(series['wave_centre'][0],series['wave_centre'][1],text,horizontalalignment='center',verticalalignment='center',rotation=text_rotation,size=text_size)
            
        #axes.set_xbound(edges[0],edges[-1])
        