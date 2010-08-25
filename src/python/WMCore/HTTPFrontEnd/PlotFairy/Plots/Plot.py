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
            print self.__class__
            print self.__class__.__bases__
            print self.__class__.__metaclass__
            print self.__class__.__mro__
            
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
            super(self.__class__,self).__init__()
        if not '__init__' in attrs:
            attrs['__init__']=__init__
        
        class Props:
            def get(self,name,default=None):
                if not hasattr(self,name):
                    return default
                return getattr(self,name)
        attrs['props']=Props()
        attrs['validators']=[]
        attrs['figure']=None
        return super(Plot,cls).__new__(cls, name, bases, attrs)


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

    

	
	
    	
    