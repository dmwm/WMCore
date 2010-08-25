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
            {label:'Frogs',value:15,colour:'#ffffff'},
            {label:'Hogs',value:30,colour:'#ffffff'}
          ]
          explode:1  
        }
        """
        
        xy = (input['width']/input.get('dpi',96), 
              input['height']/input.get('dpi',96))
        fig = figure(figsize=xy, dpi=input.get('dpi',96))
        axis = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        
        labels = []
        fracs = []
        colours = []
        for s in input['series']:
            labels.append(s['label'])
            fracs.append(s['value'])
            colours.append(s['colour'])
            explode = [0] * len(fracs)
        if 'explode' in input:
            ind = input['explode'] - 1
            explode[ind] = 0.05
        axis.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', 
                 shadow=True, colors=colours)
        
        return fig    