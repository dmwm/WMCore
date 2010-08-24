import math
from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import matplotlib.ticker
import numpy as np

from Plot import Plot
from Mixins import *
from Validators import *

class Baobab(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin):
    '''
    A hierarchical pie chart, as per GNOME Baobab and KDE Filelight.
    
    The plot consists of concentric circles which are progressively subdivided
    into smaller and smaller elements. This works very well for visualising
    directory structures in filesystems to show overall space usage, but
    can be applied to any other form of subdividable data.
    
    The data should be a nested series of dictionaries, of the form:
    
    {
     'label': 'root',
     'value': 1000,
     'children': [
                  {'label': 'child',
                   'value':500,
                   'children':[]
                   }
                  ]
    }
     
    Nodes may optionally include 'height' or 'colour', to override the defaults.
    
    
    etc, etc. This data should always include a root element reflecting the
    total volume, but which will not be shown. Elements from depth=1 will be
    displayed. At each level, the sum of child values must be less than or
    equal the parent value. Negative values should also not be used.
    
    At each level, only elements which would appear larger than `minpixel' are
    actually displayed. All others are merged together into a reduced-height
    block.
    
    The plotter attempts to draw the label for each node inside the area, after
    it has been truncated appropriately. The optimum of radial and tangential is
    picked depending on available length, and the text size determined
    appropriately.
    
    This plot may sometimes cause Agg to complain about excessive rendering
    complexity. The exact scenario this happens in is not understood yet,
    try changing the plot size, minpixel, minimum label size or disable
    labelled altogether.
    '''
    __metaclass__=Plot
    def __init__(self):
        self.validators = [IntBase('minpixel',min=0,default=10,doc_user="Sectors below this size are merged together."),
                           ElementBase('scale',bool,default=True,doc_user="Show a scale round the outside."),
                           StringBase('unit',None,default='',doc_user="Unit to append to numbers."),
                           StringBase('format',('num','si','binary'),default='si',doc_user="Formatter to use for values."),
                           ColourBase('dropped_colour',default='#cccccc',doc_user="Colour for merged nodes."),
                           ElementBase('external',bool,default=True,doc_user="Whether to draw text for the outer ring outside of the diagram."),
                           IntBase('scale_number',min=1,default=5,doc_user="Number of scale points to draw."),
                           ElementBase('labelled',bool,default=True,doc_user="Draw labels inside each node, if possible."),
                           ElementBase('central_label',bool,default=True,doc_user="Draw a label in the centre showing diagram total."),
                           FloatBase('dropped_colour_size',min=0,max=1,default=0.75,doc_user="Relative height of merged nodes."),
                           IntBase('text_truncate_inner',default=-1,doc_user="Maximum length of text drawn inside nodes."),
                           IntBase('text_truncate_outer',default=-1,doc_user="Maximum length of text drawn outside the circle."),
                           IntBase('text_size_min',min=1,default=4,doc_user="Minimum text size to render. Text smaller than this is not drawn.")]
        self.props = Props()
        super(Baobab,self).__init__(Axes_Projection='polar',Axes_Square=True,Padding_Left=50,Padding_Right=50,Padding_Bottom=50)
    def validate(self,input):
       if not 'data' in input:
           return "No 'data' element."
       data = input['data']
       if data.get('value',0)<=0: 
           return "Root element value is 0 or negative."
       if len(data.get('children',[]))==0:
           return "Root element has no children."
       return True
    def extract(self,input):
        self.props.data = input['data']
    def predata(self):
        axes = self.figure.gca()
        axes.set_axis_off()
         
        def depth_recursor(here):
            if len(here.get('children',[]))>0:
                return max([depth_recursor(c) for c in here.get('children',[])])+1
            return 1
        
        self.props.max_depth = depth_recursor(self.props.data)
        
    def data(self):
        
        axes = self.figure.gca()

        theta = lambda x: x*(2*math.pi)/self.props.data.get('value',0)
        rad = lambda depth, radians: ((((float(depth)+2)/(self.props.max_depth+3))*self.props.width)/2.)*radians
        
        if self.props.format=='binary':
            locator = BinaryMaxNLocator(self.props.scale_number)
            formatter = BinFormatter()
        elif self.props.format=='si':
            locator = matplotlib.ticker.MaxNLocator(self.props.scale_number)
            formatter = SIFormatter()
        else:
            locator = matplotlib.ticker.MaxNLocator(self.props.scale_number)
            formatter = lambda x: str(x)
        
        def bar_recursor(depth,here,startx):
            if len(here.get('children',[]))>0:
                left=[theta(startx)]
                height=[here.get('height',1)]
                width=[theta(here.get('value',0))]
                bottom=[depth]
                name=[here.get('label','')]
                value=[here.get('value',0)]
                colour=[here.get('colour',None)]
                dumped = 0
                for c in here.get('children',[]):
                    if rad(depth,theta(c.get('value',0)))>self.props.minpixel:
                        cleft,cheight,cwidth,cbottom,cname,cvalue,ccolour = bar_recursor(depth+1,c,startx)
                        left.extend(cleft)
                        height.extend(cheight)
                        width.extend(cwidth)
                        bottom.extend(cbottom)
                        name.extend(cname)
                        value.extend(cvalue)
                        colour.extend(ccolour)
                        startx += c.get('value',0)
                    else:
                        dumped += c.get('value',0)
                if dumped>0:
                    left.append(theta(startx))
                    height.append(0.75)
                    width.append(theta(dumped))
                    bottom.append(depth+1)
                    name.append('')
                    value.append(dumped)
                    colour.append('dumped_colour')
                return left,height,width,bottom,name,value,colour
            else:
                return [theta(startx)],[here.get('height',1)],[theta(here.get('value',0))],[depth],[here.get('label','')],[here.get('value',0)],[here.get('colour',None)]
    
        left,height,width,bottom,name,value,colours = bar_recursor(0,self.props.data,0)
        
        for i,l in enumerate(left):
            if colours[i]==None:
                colours[i] = self.props.colourmap(l/(2*math.pi))
            elif colours[i]=='dumped_colour':
                colours[i] = self.props.dropped_colour
        
        self.props.max_height = max(bottom)
    
        unit = self.props.unit
        
        if self.props.scale:
            
            boundaries = locator.bin_boundaries(0,self.props.data.get('value',0))[:-1]
            for b in boundaries:
                lx = theta(b)
                if self.props.external:
                    ly = self.props.max_height+3
                else:
                    ly = self.props.max_height+1.5
                line = Line2D((lx,lx),(0.75,ly),linewidth=1,linestyle='-.',zorder=-2,color='blue')
                axes.add_line(line)
                axes.text(lx,ly,formatter(b)+unit,horizontalalignment='center',verticalalignment='center',zorder=-1,color='blue')
        else:
            if self.props.external:
                axes.add_line(Line2D((0,0),(0.75,self.props.max_height+3),linewidth=0,alpha=0,zorder=-3))
            else:
                axes.add_line(Line2D((0,0),(0.75,self.props.max_height+1.5),linewidth=0,alpha=0,zorder=-3))
    
        bars = axes.bar(left=left[1:],height=height[1:],width=width[1:],bottom=bottom[1:],color=colours[1:])

        if self.props.labelled:
            max_height = max(bottom[1:])
            min_height = min(bottom[1:])
            for l,h,w,b,n,v in zip(left[1:],height[1:],width[1:],bottom[1:],name[1:],value[1:]):
                cx = l+w*0.5
                cy = b+h*0.5
                
                angle_deg = cx*(180/math.pi)
                angle_rad = angle_deg
                angle_tan = angle_deg - 90
                
                if angle_deg>90 and angle_deg<=180:
                    angle_rad += 180
                if angle_deg>180 and angle_deg<=270:
                    angle_rad -= 180
                    angle_tan -= 180
                if angle_deg>270 and angle_deg<=360:
                    angle_tan -= 180
                if self.props.external:
                    radial_length = (1./(max_height+3))*(self.props.width/2)
                else:
                    radial_length = (1./(max_height+1.5))*(self.props.width/2)
                if b==max_height and self.props.external:
                    if self.props.text_truncate_outer != -1 and len(n)>=self.props.text_truncate_outer:
                        n = n[:self.props.text_truncate_outer-1]+'..'
                    tangential_length = w*(max_height+0.5)*radial_length
                    text_size=min(font_size(n,2*radial_length),font_size('',tangential_length))
                    if text_size>=self.props.text_size_min:
                        axes.text(cx,cy+2.0,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=text_size)
                else:
                    if self.props.text_truncate_inner != -1 and len(n)>=self.props.text_truncate_inner:
                        n = n[:self.props.text_truncate_inner-1]+'..'
                    tangential_length = min(w*(b+.33)*radial_length,2*radial_length*math.sqrt(1.33*b+0.88))#h+0.75))
                    if tangential_length>radial_length:
                        text_size = min(font_size('',radial_length),font_size(n,tangential_length))
                        if text_size>=self.props.text_size_min:
                            axes.text(cx,cy-0.16,n,horizontalalignment='center',verticalalignment='center',rotation=angle_tan,size=text_size)
                    else:
                        text_size = min(font_size('',tangential_length),font_size(n,radial_length))
                        if text_size>=self.props.text_size_min:
                            axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=text_size)
            if self.props.central_label:
                axes.text(0,0,formatter(self.props.data.get('value',0))+unit,horizontalalignment='center',verticalalignment='center',weight='bold')
        