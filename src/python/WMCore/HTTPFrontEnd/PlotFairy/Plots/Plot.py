#!/usr/bin/env python
import re
import matplotlib
import matplotlib.colors
import matplotlib.image
import matplotlib.ticker
from matplotlib.pyplot import figure
'''
The Plot class is a base class for PlotFairy plots to inherit from. Authors of
new plots should override the plot(self, data) method. Plots should be 
instantiated via a factory, and be stateless.
'''
from matplotlib import pyplot 
class Plot(object):
    def __call__(self, input):
        '''
        Plot objects will be instantiated either directly or via a factory. 
        They will then be called to create the plot for some given input data.
        We use the __call__ method as a way of enforcing good behaviour, and 
        hopefully minimising memory leakage. 
        '''
        self.validate_input(input)
        plot = self.plot(input)
        
        #watermark
        
        if input.get('watermark',False):
        	plot = self.watermark(input,plot)    
            
        pyplot.close()
        return plot
        
    def plot(self):
        '''
        Create the matplotlib object and return it - override!
        '''
        raise NotImplemented
    
    def watermark(self,input,figure):
    
    	valid_watermarks = {'cms':'cmslogo.png'}
    
        watermark = input.get('watermark',False)
        watermark_alpha = input.get('watermark_alpha',0.5)
        watermark_rect = input.get('watermark_rect',(0.05,0.8,0.15,0.15))
        
        if watermark in valid_watermarks:
            try:
                im = matplotlib.image.imread(valid_watermarks[watermark]) #this only supports PNG, but actually works
                im[:,:,-1] = watermark_alpha
                axes = figure.add_axes(watermark_rect)
                axes.set_axis_off()
                axes.imshow(im)
            except:
                pass
            
        return figure
    
    def validate_input(self, input):
        """
        Dummy function to override to validate specific input for a plot.
        """
        return input
    
    def getfig(self, input):
        xy = (input['width']/input.get('dpi',96), 
              input['height']/input.get('dpi',96))
        fig = figure(figsize=xy, dpi=input.get('dpi',96))
        return fig
    
def siformat(val, unit='', long=False):
    suffix = [(1e18,'E','exa'), (1e15,'P','peta'), (1e12,'T','tera'),
                  (1e9,'G','giga'), (1e6,'M','mega'), (1e3,'k','kilo'),
                  (1,'',''), (1e-3,'m','mili'), (1e-6,'u','micro'),
                  (1e-9,'n','nano'),(1e-12,'p','pico'),(1e-15,'f','femto'),
                  (1e-18,'a','atto')]
    use = 1
    if long:
        use = 2
    for s in suffix:
        if abs(val)>=100*s[0]:
            return "%.0f%s%s"%(val/s[0],s[use],unit)
        if abs(val)>=10*s[0]:
            return "%.1f%s%s"%(val/s[0],s[use],unit)
        if abs(val)>=s[0]:
            return "%.2f%s%s"%(val/s[0],s[use],unit)
    return str(val)

def binformat(val,unit='',long=False):
    suffix = [(2**60,'E','exa'),(2**50,'P','peta'),(2**40,'T','tera'),(2**30,'G','giga'),(2**20,'M','mega'),(2**10,'k','kilo'),(1,'','')]
    use = 1
    if long:
      use = 2
    for s in suffix:
        if abs(val)>=100*s[0]:
            return "%.0f%s%s"%(val/s[0],s[use],unit)
        if abs(val)>=10*s[0]:
            return "%.1f%s%s"%(val/s[0],s[use],unit)
        if abs(val)>=s[0]:
            return "%.2f%s%s"%(val/s[0],s[use],unit)
    return str(val)
    
def validate_colour(c):
    match = re.match('^#?([0-9A-Fa-f]{6})$',c) 
    if match:
        return '#%s'%match.group(1)
    elif c in matplotlib.colors.cnames:
        return c
    else:
        return '#000000'
    
def validate_series_item(s,default_label='',default_colour='#000000',value_type='seq'):
    if not 'label' in s:
        s['label']=default_label
    if 'colour' in s:
        s['colour'] = validate_colour(s['colour'])
    else:
        s['colour']=default_colour
    if not 'value' in s:
        if value_type=='seq':
            s['value']=[]
        elif value_type=='none':
            pass
        else:
            s['value']=0
    return s

def validate_axis(a,default_label='',default_type='num'):
    if not 'label' in a:
        a['label']=default_label
    if not 'type' in a:
        a['type']=default_type
    if a['type']=='num' or a['type']=='time':
        if not 'min' in a:
            a['min']=0
        if not 'max' in a:
            a['max']=10
        if not 'width' in a:
            a['width']=1
    if a['type']=='labels':
        if not 'labels' in a:
            a['labels'] = []
    return a

    

	
	
    	
    