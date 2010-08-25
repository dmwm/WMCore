from Plot import Plot
from Mixins import *
from Validators import *

class Sparkline(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin,BinnedNumericSeriesMixin,WatermarkMixin):
    '''
    Draw one or more small series without marked axes, intended
    for quick visualisation of trends.
    
    Multiple series can either be drawn overlaid on the same
    (invisble) axis or on separate, stacked axes. They can also
    be labelled with auto-scaled text taking up 'text_fraction'
    of the available width.
    '''
    __metaclass__=Plot
    def __init__(self):
        self.validators = [ElementBase('labelled',bool,default=True,doc_user="Draw series labels."),
                           ElementBase('overlay',bool,default=True,doc_user="Overlay multiple series on same axis."),
                           StringBase('linestyle',default='-',doc_user="Matplotlib linestyle for lines."),
                           MarkerBase('marker',default='None',doc_user="Marker style to draw."),
                           FloatBase('linewidth',min=0,default=1,doc_user="Linewidth in pixels for drawing."),
                           FloatBase('text_fraction',min=0,max=1,default=0.2,doc_user="Fraction of width to use for labels.")]
        self.props = Props()
        super(Sparkline,self).__init__(BinnedNumericSeries_BinSrc=None,Padding_Top=0,Padding_Left=0,Padding_Right=0,Padding_Bottom=0)
    def data(self):
        axes = self.figure.gca()
        axes.set_axis_off()
        if len(self.props.series)==0:
            return
        
        left = float(self.props.padding_left)/self.props.width + self.props.text_fraction
        left_nolabel = float(self.props.padding_left)/self.props.width
        bottom = float(self.props.padding_bottom)/self.props.width
        width = (1 - float(self.props.padding_right)/self.props.width - left)
        width_nolabel = float(self.props.avail_width)/self.props.width
        height = float(1 - (self.props.height-self.props.topbound) - self.props.padding_bottom)
        
        
        
        plot_height = height/len(self.props.series)
                
        text_pixels = (self.props.text_fraction*0.9)*self.props.avail_width
        text_height = float(self.props.avail_height)/len(self.props.series)
        
        text_right = float(self.props.padding_left)/self.props.width+(self.props.text_fraction*0.9)
        text_mid = [(self.props.padding_bottom+(i+0.5)*text_height)/self.props.height for i in range(len(self.props.series))]
        
        if self.props.overlay:
            for series in self.props.series:
                linestyle = self.props.linestyle if series.get('linestyle',None)==None else series['linestyle']
                marker = self.props.marker if series.get('marker',None)==None else series['marker']
                linewidth = self.props.linewidth if series.get('linewidth',None)==None else series['linewidth']
                axes.plot(series['values'],color=series['colour'],linestyle=linestyle,marker=marker,linewidth=linewidth)
            
            if self.props.labelled:
                axes.set_position((left,bottom,width,height))
                for i,series in enumerate(self.props.series):
                    label = series['label']
                    if len(label)>0:
                        text_size = min(font_size(label,text_pixels),font_size('',text_height))
                        self.figure.text(text_right,text_mid[i],label,color=series['colour'],ha='right',va='center',fontsize=text_size)
        else:
            for i,series in enumerate(self.props.series):
                if self.props.labelled:
                    axes = self.figure.add_axes([left,bottom+plot_height*i,width,plot_height])
                else:
                    axes = self.figure.add_axes([left_nolabel,bottom+text_height*i,width_nolabel,text_height])
                linestyle = self.props.linestyle if series.get('linestyle',None)==None else series['linestyle']
                marker = self.props.marker if series.get('marker',None)==None else series['marker']
                linewidth = self.props.linewidth if series.get('linewidth',None)==None else series['linewidth']
                axes.plot(series['values'],color=series['colour'],linestyle=linestyle,marker=marker,linewidth=linewidth)
                axes.set_axis_off()
            if self.props.labelled:
                for i,series in enumerate(self.props.series):
                    label = series['label']
                    if len(label)>0:
                        text_size = min(font_size(label,text_pixels),font_size('',text_height))
                        #print 'text_size',text_size
                        #print 'text_right',text_right,'text_mid',text_mid[i]
                        self.figure.text(text_right,text_mid[i],label,color=series['colour'],ha='right',va='center',fontsize=text_size)
                
                
