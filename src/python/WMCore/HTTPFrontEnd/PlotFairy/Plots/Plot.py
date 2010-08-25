#!/usr/bin/env python
import re
import matplotlib
'''
The Plot class is a base class for PlotFairy plots to inherit from. Authors of
new plots should override the plot(self, data) method. Plots should be 
instantiated via a factory, and be stateless.
'''
import matplotlib.pyplot 
from Utils import Props


class Plot(type):
    def __new__(cls, name, bases, attrs):
        #print 'Plot::__new__',cls,name,bases
        def _validate(self,input):
            for v in self.validators:
                if not v.validate(input):
                    return 'validation-failed: %s'%v.element_name
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
            self.figure = None
            validate = self._validate(input)
            if validate==True:
                try:
                    self._extract(input)
                    self._construct()
                    self._predata()
                    self._data()
                    self._postdata()
                    self._finalise()
                    return self.figure
                except Exception as e:
                    return self._error(str(e))
            return self._error(validate)
        attrs['__call__']=__call__
        
        def __init__(self,*args,**kwargs):
            self.props = Props()
            self.validators = []
            self.figure = None
            super(self.__class__,self).__init__(*args,**kwargs)
        if not '__init__' in attrs:
            attrs['__init__']=__init__
        
        def _error(self,msg):
            height = self.props.get('height',600)
            width = self.props.get('width',800)
            dpi = self.props.get('dpi',96)
            if self.figure and isinstance(self.figure,matplotlib.figure.Figure):
                try:
                    matplotlib.pyplot.close(self.figure)
                except:
                    pass
            self.figure = matplotlib.pyplot.figure(figsize=(self.props.width/self.props.dpi,self.props.height/self.props.dpi),dpi=self.props.dpi)
            self.figure.text(0.5,0.5,'Error!\n%s'%msg,ha='center',va='center',weight='bold',color='r')
            return self.figure
            
        if not '_error' in attrs:
            attrs['_error']=_error
        def __del__(self):
            if self.figure and isinstance(self.figure,matplotlib.figure.Figure):
                try:
                    matplotlib.pyplot.close(self.figure)
                except:
                    pass
        if not '__del__' in attrs:
            attrs['__del__']=__del__
        return super(Plot,cls).__new__(cls, name, bases, attrs)