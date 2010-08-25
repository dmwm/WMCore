from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
import math
from Plot import Plot, validate_axis, validate_series_item

class Cumulative(Plot):
    def validate_input(self,input):
        if not 'xaxis' in input:
            input['xaxis']={}
        input['xaxis']=validate_axis(input['xaxis'])
        if not 'yaxis' in input:
            input['yaxis']={}
        if not 'series' in input:
            input['series']=[]
        else:
            newseries=[]
            for i,item in enumerate(input['series']):
                newseries.append(validate_series_item(item,default_colour=cm.Dark2(float(i)/len(input['series']))))
            input['series']=newseries
        return input
        
    def plot(self,input):
        """
        Draw a cumulative plot. The argument and optional arguments for this are identical to the numerical bar chart case above.
        """
        fig = self.getfig(input)
        
        axes = fig.add_axes([0.1,0.1,0.8,0.8])
        axes.set_title(input.get('title',''))
        xaxis = input['xaxis']
        yaxis = input['yaxis']
    
        axes.set_xlabel(xaxis.get('label',''))
        axes.set_ylabel(yaxis.get('label',''))

        xtype = xaxis['type']
        series = input['series']
        
        y_min = 0
        y_max = 0
        if yaxis.get('log',False):
            axes.set_yscale('log')
            if len(series)>0:
                y_min = min(filter(lambda x: x>0, series[0]['values']))
                y_min = 10**int(math.log10(y_min))
        
        x_min = xaxis.get('min',0)
        x_max = xaxis.get('max',1)
        x_width = xaxis.get('width',1)
        x_range = x_max-x_min
        x_bins = int(x_range/x_width)
        
        y0 = [y_min for i in range(x_bins)]
        x = [x_min+(i+1)*x_width for i in range(x_bins)]
        
        for s in series:
            height = s['values']
            assert len(height)==x_bins
            colour = s['colour']
            y1 = [y+h for y,h in zip(y0,height)]
            y_max = max(y1)
            axes.fill_between(x,y1,y0,label=s['label'],facecolor=colour)
            y0 = y1
            
        if yaxis.get('log',False):
            y_max = 10**int(math.log10(y_max))+1
        
        if xtype=='time':
            axes.xaxis_date()                
        axes.set_xbound(x_min,x_max)
        axes.set_ybound(y_min,y_max)
        
        if input.get('grid',False):
            axes.grid()
        
        if input.get('legend',False):
            axes.legend([Rectangle((0,0),1,1,fc=s['colour']) for s in series],[s['label'] for s in series],loc=0)
        return fig