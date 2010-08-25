import math
from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import numpy as np
from Plot import Plot, siformat, binformat

class Baobab(Plot):
    def validate_input(self,input):
        if not 'data' in input:
            input['data']={'label':'root','value':1,'children':[]}
        return input
        
    def plot(self,input):
        """
        Make a baobab/filelight hierarchical pie chart.

        Requires input:
        { width, height, title...
          data: {
            label:'root element (not drawn)',
            value:1000,
            children:[
              {label: 'top level element', value: 500, children: []},
              {label: 'top level element2', value: 250, children: [
                {label: 'second level element', value: 100, children: []}
              ]}
            ]
          },
        }
        The sum of all first-level child elements must be less than or equal to the parent value.
        Optional top-level elements include
        'labelled':False - draw names on each element
        'cm':'name of a matplotlib colourmap' - colouring to use for elements
        'threshold':0.05 - threshold of parent value below which children are culled to unclutter the plot
        """
        
        fig = self.getfig(input)
    
        axes = fig.add_axes([0.05,0.05,0.9,0.8],polar=True)
        axes.set_aspect(1,'box','C')
        axes.set_title(input.get('title',''))
        axes.set_axis_off()
    
        data_root = input['data']
        cmap = input.get('cm','Accent')
        cmap = cm.get_cmap(cmap)
        if not cmap:
            cmap = cm.Accent 
    
        theta = lambda x: x*(2*math.pi)/data_root['value']
        #threshold = input.get('threshold',0.05)
        minpixel = input.get('minpixel',10)
        
        def depth_recursor(here):
            if len(here['children'])>0:
                return max([depth_recursor(c) for c in here['children']])+1
            return 1
            
        max_depth = depth_recursor(data_root)
        rad = lambda depth, radians: ((((float(depth)+2)/(max_depth+3))*input['width'])/2.)*radians
    
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
                    if rad(depth,theta(c['value']))>minpixel:
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
    
        left,height,width,bottom,name,value = bar_recursor(0,data_root,0)
    
        colours = [cmap(l/(2*math.pi)) for l in left]
        for i,h in enumerate(height):
            if h<1:
                colours[i] = '#cccccc'
    
        max_height = max(bottom)
    
        unit = input.get('unit','')
        
        if input.get('scale',False):
            bar_location = data_root['value']/5.
            magnitude = int(math.log10(bar_location))
            possible = [i*10**magnitude for i in (1.,2.,5.,10.)]
            best_delta = 10*10**magnitude
            use_bar_location = 0
            for p in possible:
                if abs(p-bar_location)<best_delta:
                    best_delta=abs(p-bar_location)
                    use_bar_location=p
    
            for i in range(int(data_root['value']/use_bar_location)+1):
                lx = theta(i*use_bar_location)
                ly = max_height+3
            
                line = Line2D((lx,lx),(0.75,ly),linewidth=1,linestyle='-.',zorder=-2,color='blue')
                axes.add_line(line)
                axes.text(lx,ly,siformat(i*use_bar_location,unit),horizontalalignment='center',verticalalignment='center',zorder=-1,color='blue')
        else:
            axes.add_line(Line2D((0,0),(0.75,max_height+3),linewidth=0,alpha=0,zorder=-3))
            #axes.set_ybound(0,max_height+3)
            
    
        bars = axes.bar(left=left[1:],height=height[1:],width=width[1:],bottom=bottom[1:],color=colours[1:])

        def fontsize(pix,chars):
            print pix, chars,int(pix/((2+0.6*chars)*(input.get('dpi',96)/72.))) 
            return int(pix/((2+0.6*chars)*(input.get('dpi',96)/72.)))

        if input.get('labelled',False):
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
                radial_text_length = (1./(max_height+3))*input['width']/2              
                if b==max_height:
                    max_text_length = (2.5/(max_height+3))*input['width']/2
                    max_text_height = w*((max_height+0.5)/(max_height+3))*input['width']/2
                    axes.text(cx,cy+1.5,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=min(fontsize(max_text_length,len(n)),fontsize(max_text_height,-1)))
                elif b==min_height:
                    max_text_length = min(w,math.pi/2)*(h+.5)/(max_height+3)*input['width']/2
                    axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_tan,size=fontsize(max_text_length,len(n)))
                else:
                    axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=fontsize(radial_text_length,len(n)))
    
            axes.text(0,0,siformat(data_root['value'],unit),horizontalalignment='center',verticalalignment='center',weight='bold')
    
        return fig
    