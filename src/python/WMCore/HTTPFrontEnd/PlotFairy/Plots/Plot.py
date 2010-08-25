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

from Utils import *



class Plot(type):
    def __new__(cls, name, bases, attrs):
        def _validate(self,input):
            for v in self.validators:
                if not v.validate(input):
                    print 'failed: %s'%v.element_name
                    return False
            return super(self.__class__,self).validate(input) and self.validate(input)
        attrs['_validate']=_validate
        if not 'validate' in attrs:
            attrs['validate']=lambda self,input: True
        def _extract(self,input):
            for v in self.validators:
                if hasattr(self.props,v.element_name):
                    val = v.extract(input)
                    if isinstance(val,dict):
                        getattr(self.props,v.element_name).update(val)
                    else:
                        setattr(self.props,v.element_name,val)
                else:
                    setattr(self.props,v.element_name,v.extract(input))
            super(self.__class__,self).extract(input)
            self.extract(input)
        attrs['_extract']=_extract
        if not 'extract' in attrs:
            attrs['extract']=lambda self,input: None
        def _construct(self):
            super(self.__class__,self).construct()
            self.construct()
        attrs['_construct']=_construct
        if not 'construct' in attrs:
            attrs['construct']=lambda self: None
        def _predata(self):
            super(self.__class__,self).predata()
            self.predata()
        attrs['_predata']=_predata
        if not 'predata' in attrs:
            attrs['predata']=lambda self: None
        def _data(self):
            super(self.__class__,self).data()
            self.data()
        attrs['_data']=_data
        if not 'data' in attrs:
            attrs['data']=lambda self: None
        def _postdata(self):
            super(self.__class__,self).postdata()
            self.postdata()
        attrs['_postdata']=_postdata
        if not 'postdata' in attrs:
            attrs['postdata']=lambda self: None
        def _finalise(self):
            super(self.__class__,self).finalise()
            self.finalise()
        attrs['_finalise']=_finalise
        if not 'finalise' in attrs:
            attrs['finalise']=lambda self: None
        def __call__(self,input):
            if self._validate(input):
                self._extract(input)
                self._construct()
                self._predata()
                self._data()
                self._postdata()
                self._finalise()
                return self.figure
            return None
        attrs['__call__']=__call__
        
        def __init__(self):
            self.props = Props()
            self.validators = []
            super(self.__class__,self).__init__()
        if not '__init__' in attrs:
            attrs['__init__']=__init__
        
        #attrs['props']=Props()
        #attrs['validators']=[]
        attrs['figure']=None
        return super(Plot,cls).__new__(cls, name, bases, attrs)
