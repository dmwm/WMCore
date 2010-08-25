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