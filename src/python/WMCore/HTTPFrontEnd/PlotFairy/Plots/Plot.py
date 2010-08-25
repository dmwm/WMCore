#!/usr/bin/env python
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
        plot = self.plot(input)
        pyplot.close()
        return plot
        
    def plot(self):
        '''
        Create the matplotlib object and return it - override!
        '''
        return None
    
    def siformat(self, val, unit='', long=False):
        suffix = [(1e18,'E','exa'),(1e15,'P','peta'),(1e12,'T','tera'),(1e9,'G','giga'),(1e6,'M','mega'),(1e3,'k','kilo'),(1,'',''),(1e-3,'m','mili'),(1e-6,'u','micro'),(1e-9,'n','nano'),(1e-12,'p','pico'),(1e-15,'f','femto'),(1e-18,'a','atto')]
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
