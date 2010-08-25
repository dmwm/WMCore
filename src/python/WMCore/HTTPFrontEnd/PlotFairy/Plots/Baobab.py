from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot

class Baobab(Plot):
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
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
    
        axes = fig.add_axes([0.1,0.1,0.8,0.8],polar=True)
        axes.set_title(input.get('title',''))
        axes.set_axis_off()
    
        data_root = input['data']
        cmap = input.get('cm','Accent')
        cmap = cm.get_cmap(cmap)
        if not cmap:
            cmap = cm.Accent 
    
        theta = lambda x: x*(2*math.pi)/data_root['value']
        threshold = input.get('threshold',0.05)
        
        
    
        def fontsize(n):
            if len(n)>16:
                return 6
            if len(n)>8:
                return 8
            return 10
     
    
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
                    if theta(c['value'])>(9./(depth+1)**2)*math.pi/180:
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
            
    
        font = FontProperties()
    
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
                axes.text(lx,ly,SIFormatter(i*use_bar_location,unit),horizontalalignment='center',verticalalignment='center',zorder=-1,color='blue')
    
    
        bars = axes.bar(left=left[1:],height=height[1:],width=width[1:],bottom=bottom[1:],color=colours[1:])
    
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
                if b==max_height:
                    axes.text(cx,cy+1.5,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=fontsize(n))
                elif b==min_height:
                    axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_tan,size=fontsize(n))
                else:
                    axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=fontsize(n))
    
            axes.text(0,0,SIFormatter(data_root['value'],unit),horizontalalignment='center',verticalalignment='center',weight='bold')
    
        return fig
    