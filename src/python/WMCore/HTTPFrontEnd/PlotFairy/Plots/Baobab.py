import math
from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import matplotlib.ticker
import numpy as np

from Plot import Plot
from Utils import *

class Baobab(FigureMixin,TitleMixin,FigAxesMixin,StyleMixin):
    __metaclass__=Plot
    def __init__(self):
        self.validators = [IntBase('minpixel',min=0,default=10),
                           ElementBase('scale',bool,default=True),
                           StringBase('unit',None,default=''),
                           StringBase('format',('num','si','binary'),default='si'),
                           ColourBase('dropped_colour',default='#cccccc'),
                           ElementBase('external',bool,default=True),
                           IntBase('scale_number',min=1,default=5),
                           ElementBase('labelled',bool,default=True),
                           ElementBase('central_label',bool,default=True)]
        self.props = Props()
        super(Baobab,self).__init__(Axes_Projection='polar',Axes_Square=True,Padding_Left=50,Padding_Right=50,Padding_Bottom=50)
    def validate(self,input):
       if not 'data' in input:
           return False
       return True
    def extract(self,input):
        self.props.data = input['data']
    def predata(self):
        axes = self.figure.gca()
        axes.set_axis_off()
         
        def depth_recursor(here):
            if len(here['children'])>0:
                return max([depth_recursor(c) for c in here['children']])+1
            return 1
        
        self.props.max_depth = depth_recursor(self.props.data)
        
    def data(self):
        
        axes = self.figure.gca()
        
        theta = lambda x: x*(2*math.pi)/self.props.data['value']
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
            if len(here['children'])>0:
                left=[theta(startx)]
                height=[1]
                width=[theta(here['value'])]
                bottom=[depth]
                name=[here['label']]
                value=[here['value']]
                dumped = 0
                for c in here['children']:
                    #if c['value']>here['value']*threshold:
                    #if theta(c['value'])>(9./(depth+1)**2)*math.pi/180:
                    if rad(depth,theta(c['value']))>self.props.minpixel:
                        cleft,cheight,cwidth,cbottom,cname,cvalue = bar_recursor(depth+1,c,startx)
                        left.extend(cleft)
                        height.extend(cheight)
                        width.extend(cwidth)
                        bottom.extend(cbottom)
                        name.extend(cname)
                        value.extend(cvalue)
                        startx += c['value']
                    else:
                        dumped += c['value']
                if dumped>0:
                    left.append(theta(startx))
                    height.append(0.75)
                    width.append(theta(dumped))
                    bottom.append(depth+1)
                    name.append('')
                    value.append(dumped)
                return left,height,width,bottom,name,value
            else:
                return [theta(startx)],[1],[theta(here['value'])],[depth],[here['label']],[here['value']]
    
        left,height,width,bottom,name,value = bar_recursor(0,self.props.data,0)
        
        colours = [self.props.colourmap(l/(2*math.pi)) for l in left]
        for i,h in enumerate(height):
            if h<1:
                colours[i] = self.props.dropped_colour
    
        self.props.max_height = max(bottom)
    
        unit = self.props.unit
        
        if self.props.scale:
            
            boundaries = locator.bin_boundaries(0,self.props.data['value'])
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
                    tangential_length = w*(max_height+0.5)*radial_length
                    axes.text(cx,cy+2.0,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=min(font_size(n,2*radial_length),font_size('',tangential_length)))
                else:
                    tangential_length = min(w*(b+.33)*radial_length,2*radial_length*math.sqrt(1.33*b+0.88))#h+0.75))
                    if tangential_length>radial_length:
                        axes.text(cx,cy-0.16,n,horizontalalignment='center',verticalalignment='center',rotation=angle_tan,size=min(font_size('',radial_length),font_size(n,tangential_length)))
                    else:
                        axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=min(font_size('',tangential_length),font_size(n,radial_length)))
            if self.props.central_label:
                axes.text(0,0,formatter(self.props.data['value'])+unit,horizontalalignment='center',verticalalignment='center',weight='bold')
    




    