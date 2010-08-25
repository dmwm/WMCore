from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
import numpy as np
from Plot import Plot

class Pie(Plot):
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
        
        xy = (input['width']/input.get('dpi',96), input['height']/input.get('dpi',96))
        fig = figure(figsize = xy)
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
        axis.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True,colors=colours)
        
        return fig    