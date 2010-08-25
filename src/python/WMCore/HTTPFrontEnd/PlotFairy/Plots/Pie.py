from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot, validate_series_item

class Pie(Plot):
    def validate_input(self,input):
        if not 'series' in input:
            input['series']=[]
        else:
            newseries=[]
            for i,item in enumerate(input['series']):
                newseries.append(validate_series_item(item,default_colour=cm.Dark2(float(i)/len(input['series'])),value_type='notseq'))
            input['series']=newseries
        return input
    def plot(self, input):
        """
        Produce an object representing a pie chart
                
        { width, height, title etc
          series:[
            {label:'Frogs',value:15,colour:'#ffffff',explode=(True|0.0-1.0)},
            {label:'Hogs',value:30,colour:'#ffffff'}
          ]
          explode:1  
        }
        """
        
        fig = self.getfig(input)
        axis = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        
        
        
        labels = []
        fracs = []
        colours = []
        explode = []
        for s in input['series']:
            labels.append(s['label'])
            fracs.append(s['value'])
            colours.append(s['colour'])
            explode.append(s.get('explode',0.))
        
        if 'cm' in input:
            cmap = cm.get_cmap(input['cm'])
            if not cmap:
                cmap = cm.Accent
            colours = [cm.Dark2(float(i)/len(colours)) for i in range(len(colours))]    
        
        if 'explode' in input:
            if input['explode']=='ALL':
                explode = [0.1] * len(fracs)
            elif input['explode'] in labels:
                explode[label.index(input['explode'])] = 0.1
            elif isinstance(input['explode'],int):
                explode[input['explode']] = 0.1
        axis.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', 
                 shadow=True, colors=colours)
        
        return fig    