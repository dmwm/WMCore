#!/usr/bin/env python
import re
import matplotlib
import sys
import traceback
'''
The Plot class is a base class for PlotFairy plots to inherit from. Authors of
new plots should override the plot(self, data) method. Plots should be 
instantiated via a factory, and be stateless.
'''
import matplotlib.pyplot 
from Utils import Props,elem

class null(object):
    def null(self):
        pass
instancemethod = type(null.null)

class Plot(type):
    def __new__(cls, name, bases, attrs):
        #print 'Plot::__new__',cls,name,bases
        def _validate(self,input):
            for v in self.validators:
                if not v.validate(input):
                    return 'validation-failed: %s'%v.element_name
            return True
            #return super(self.__class__,self).validate(input) and self.validate(input)
        attrs['_validate']=_validate
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
            #super(self.__class__,self).extract(input)
            for method in self.__class__._extract_calls:
                method(self,input)
            #self.extract(input)
        attrs['_extract']=_extract
        def __call__(self,input):
            self.figure = None
            try:
                validate_result = self._validate(input)
                if not validate_result==True:
                    return self._error(validate_result)
                for method in self.__class__._validate_calls:
                    validate_result = method(self,input)
                    if not validate_result==True:
                        return self._error(validate_result)
                self._extract(input)
                for method in self.__class__._extract_calls:
                    method(self,input)
                for method in self.__class__._build_calls:
                    method(self)
                return self.figure
            except Exception as e:
                traceback.print_exc(file=sys.stderr)
                return self._error(str(e))
                #return self._error(traceback.format_exc())
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
            dpi = self.props.get('dpi',100)
            if self.figure and isinstance(self.figure,matplotlib.figure.Figure):
                try:
                    matplotlib.pyplot.close(self.figure)
                except:
                    pass
            self.figure = matplotlib.pyplot.figure(figsize=(height/dpi,width/dpi),dpi=dpi)
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
        _validate_calls = []
        _extract_calls = []
        _build_calls = []
        
        def doc(self):
            head = elem('head',elem('title','Plotfairy::Documentation::%s'%self.__class__.__name__))
            header = elem('div',elem('h1','Documentation for Plotfairy::%s'%self.__class__.__name__))
            docstr = elem('div',elem('h2','Synopsis')) \
                    +elem('div',elem('pre',self.__doc__))
            validators = elem('div',elem('h2','Options')) \
                    +elem('div',elem('ul','\n'.join([v.doc() for v in self.validators])))
            mixins = elem('div',elem('h2','Mixins')) \
                    +elem('div','Uses'+elem('ul','\n'.join([elem('li',k.__name__) for k in self.__class__.__bases__]))) \
                    +elem('div','Method order'+elem('ul','\n'.join([elem('li','%s::%s'%(f.im_class.__name__ if hasattr(f,'im_class') else self.__class__.__name__,f.__name__)) for f in self._validate_calls+self._extract_calls+self._build_calls])))
            return elem('html',head+elem('body',header+docstr+validators+mixins))        
            
        attrs['doc']=doc
        
        for klass in bases:
            if hasattr(klass,'validate'):
                _validate_calls.append(getattr(klass,'validate'))
            if hasattr(klass,'extract'):
                _extract_calls.append(getattr(klass,'extract'))
        if 'validate' in attrs:
            _validate_calls.append(attrs['validate'])
        if 'extract' in attrs:
            _extract_calls.append(attrs['extract'])
        for step in ('construct','predata','data','postdata','finalise'):
            for klass in bases:
                if hasattr(klass,step):
                    _build_calls.append(getattr(klass,step))
            if step in attrs:
                _build_calls.append(attrs[step])
        attrs['_validate_calls']=_validate_calls
        attrs['_extract_calls']=_extract_calls
        attrs['_build_calls']=_build_calls
        return super(Plot,cls).__new__(cls, name, bases, attrs)