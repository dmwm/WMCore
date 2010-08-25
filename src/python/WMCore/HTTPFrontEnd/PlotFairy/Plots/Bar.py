from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot, validate_series_item, validate_axis

class Bar(Plot):
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
                if input['xaxis']['type']=='labels':
                    newseries.append(validate_series_item(item,default_colour=cm.Dark2(float(i)/len(input['series'])),value_type='notseq'))
                else:
                    newseries.append(validate_series_item(item,default_colour=cm.Dark2(float(i)/len(input['series']))))
            input['series']=newseries
        return input
		
	
    def plot(self, input):
        """
        Produce an object representing a bar chart

        This produces two disparate types of plots. The first is a labelled bar chart containing a single series
        { width, height, title etc
          xaxis: {
            type:'labels',
            labels:['label0', 'label1', 'label2']
          },
          series: [
            {label: 'label0', value: 100, colour: '#ffffff'},
            {label: 'label1', value: 200, colour: '#ffffff'}, ...
          ]
        }
        Labels in the series must correspond to labels defined for the axis.
        
        The second case is a numerical-axis plot, which can have multiple series.
        { width, height, title etc
          xaxis: {
            type:'num',
            min:0,
            max:100,
            width:10
          },
          series: [
            {label: 'label0', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'},
            {label: 'label1', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'}, ...
          ]
        }
        Optional arguments for axes are
        'log': make the numerical axis logarithmic
        'label': axis title
        'type': 'labels' for labelled case, 'num' for general numeric case, 'time' for time formatting
        Please note matplotlib uses a daft time format whereby time x-axis values are interpreted as floating-point numbers of days since 01-01-0001. Might add a UTC converter at some point...
        Optional arguments for the plot are
        'yaxis' - to define a y axis label, log etc. Min/max/labels are ignored for this axis.
        'legend' - try and draw a legend.
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
    
        logmin = 0
        if yaxis.get('log',False):
            axes.set_yscale('log')
            if len(series)>0:
                logmin = min(1,filter(lambda x: x>0, series[0]['values']))
    
    
        if xtype=='labels':
            names = {}
            labels = xaxis['labels']
            for i,n in enumerate(labels):
                names[n]=i
            values = {}
            colours = {}
            seennames = []
            for s in series:
                assert s['label'] in names
                seennames.append(s['label'])
                values[s['label']]=s['value']
                colours[s['label']]=s['colour']
            axes.set_xticklabels(labels)
            axes.set_xticks([i+0.5 for i in range(len(labels))])
            left = [names[n] for n in seennames]
            height = [values[n] for n in seennames]
            bottom = [logmin for n in seennames]
            colour = [colours[n] for n in seennames]
            axes.bar(left,height,width=1,bottom=bottom,color=colour)
        else:
            x_min = xaxis.get('min',0)
            x_max = xaxis.get('max',1)
            x_width = xaxis.get('width',1)
            x_range = x_max-x_min
            x_bins = int(x_range/x_width)

            bottom = [logmin for i in range(x_bins)]
            width = [x_width for i in range(x_bins)]
            left = [x_min+i*x_width for i in range(x_bins)]
            for s in series:
                height = s['values']
                assert len(height)==x_bins
                colour = s['colour']
                axes.bar(left,height,width,bottom,label=s['label'],facecolor=colour)
                bottom = [b+h for b,h in zip(bottom,height)]
            
            if input.get('grid',False):
                axes.grid()
            if input.get('legend',False):
                axes.legend(loc=0)
            if xtype=='time':
                axes.xaxis_date()
                    
            axes.set_xbound(x_min,x_max)
        return fig