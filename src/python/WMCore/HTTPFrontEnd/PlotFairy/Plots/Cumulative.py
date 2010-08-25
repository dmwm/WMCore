from Plot import Plot

class Cumulative(Plot):
    def plot(self,input):
        """
        Draw a cumulative plot. The argument and optional arguments for this are identical to the numerical bar chart case above.
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
        
        axes = fig.add_axes([0.1,0.1,0.8,0.8])
        axes.set_title(input.get('title',''))
        xaxis = input.get('xaxis',{})
        yaxis = input.get('yaxis',{})
        
        axes.set_xlabel(xaxis.get('label',''))
        axes.set_ylabel(yaxis.get('label',''))
        
        xtype = xaxis.get('type','num')
        series = input.get('series',[])
        
        logmin = 0
        if yaxis.get('log',False):
            axes.set_yscale('log')
            if len(series)>0:
                logmin = min(1,filter(lambda x: x>0, series[0]['values']))
        
        x_min = xaxis.get('min',0)
        x_max = xaxis.get('max',1)
        x_width = xaxis.get('width',1)
        x_range = x_max-x_min
        x_bins = int(x_range/x_width)
        
        y0 = [logmin for i in range(x_bins)]
        x = [x_min+(i+1)*x_width for i in range(x_bins)]
        
        for s in series:
            height = s['values']
            assert len(height)==x_bins
            colour = s['colour']
            y1 = [y+h for y,h in zip(y0,height)]
            axes.fill_between(x,y1,y0,label=s['label'],facecolor=colour)
            y0 = y1
        
        if xtype=='time':
            axes.xaxis_date()                
        axes.set_xbound(x_min,x_max)
        
        if input.get('legend',False):
            axes.legend([Rectangle((0,0),1,1,fc=s['colour']) for s in series],[s['label'] for s in series],loc=0)
        return fig