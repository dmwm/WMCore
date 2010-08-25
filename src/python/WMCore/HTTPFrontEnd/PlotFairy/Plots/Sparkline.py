from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot

class Sparkline(Plot):
    def plot(self,input):
        """
        Draw an unlabelled line plot. Syntax is the same as for a pie chart.
        { width, height, title etc
          series: [
            {label: 'label0', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'},
            {label: 'label1', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'}, ...
          ]
        }
        Optional arguments are:
        'labelled': Add labels to the left of each plot.
        'overlay': Overlay the plots, instead of making a stack of separate plots.
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy, dpi=input.get('dpi',96))
    
        labelled = input.get('labelled',False)
        overlay = input.get('overlay',False)
    
        data = input['series']    
        for i,d in enumerate(data):
            if not 'colour' in d:
                d['colour'] = cm.Dark2(float(i)/len(data))
            if not 'label' in d:
                d['label'] = ''
            if not 'values' in d:
                d['values'] = [0]
    
        if len(data)>0:
            if overlay and len(data)>1:
                axes = fig.add_axes([0.1,0.1,0.9,0.9])
                miny=maxy=0
                maxlen=0
                for i,d in enumerate(data):
                    axes.plot(d['values'],color='#%s' % d['colour'])
                    miny=min(miny,min(d['values']))
                    maxy=max(maxy,max(d['values']))
                    maxlen=max(maxlen,len(d['values']))
                axes.autoscale_view()
                if labelled:
                    for i,d in enumerate(data):
                        cy = miny + (maxy-miny)*((float(i)/len(data))+(0.5/len(data)))
                        axes.text(-1,cy,d['label'],horizontalalignment='right',
                                  verticalalignment='center',fontsize=10,
                                  color='#%s' % d['colour'])
                    axes.set_xlim(xmin=-0.2*maxlen)
                axes.set_axis_off()
        
            
            else:
                for i,d in enumerate(data):
                    axes = fig.add_subplot(len(data),1,i+1)
                    axes.plot(d['values'],color='#%s' % d['colour'])
                    cy = 0.5*(max(d['values'])-min(d['values']))+min(d['values'])
                    axes.set_axis_off()    
                    if labelled:
                        axes.text(-1,cy,d['label'],horizontalalignment='right',
                                  verticalalignment='center',fontsize=10,
                                  color='#%s' % d['colour'])
                        axes.set_xlim(xmin=-0.2*len(d['values']))
        
        return fig