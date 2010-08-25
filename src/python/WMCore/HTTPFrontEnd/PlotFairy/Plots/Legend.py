import matplotlib.cm as cm
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import numpy as np
from Plot import Plot, validate_series_item

class Legend(Plot):
    def validate_input(self,input):
        if 'items' in input:
            pass
        elif 'series' in input:
            input['items'] = input['series']
        else:
            input['items']=[]
        return input
    def plot(self, input):
        """
        Produce an object representing a pie chart
                
        { width, height, title etc
          items:[
            {label:'Frogs',colour:'#ffffff'},
            {label:'Hogs',colour:'#ffffff'}
          ]
          type:line|fill
          
        }
        """
        
        
        
        fig = self.getfig(input)
        axes = fig.add_axes([0, 0, 1, 1])
        
        labels = []
        colours = []
        
        for i in input['items']:
            labels.append(i['label'])
            colours.append(i['colour'])
        
        if len(labels)>0:
            if input.get('type','fill')=='fill':
                axes.legend([Rectangle((0,0),1,1,facecolor=c) for c in colours],labels,mode='expand',loc=2)
            else:
                axes.legend([Line2D((0,1),(0,0),color=c) for c in colours],labels,mode='expand',loc=2)
        return fig