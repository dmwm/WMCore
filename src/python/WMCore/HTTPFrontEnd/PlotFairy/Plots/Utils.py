import matplotlib
import matplotlib.colors
import matplotlib.image
import matplotlib.ticker
import re
import copy
import numpy
import math
import matplotlib.pyplot
import matplotlib.cm
import time
import copy
import types
import new

#Some of this code is pinched from Overview/DQMGui plotting

def text_size(string,fontsize,dpi=96):
    return 0.6*len(string)*(dpi/72.)*fontsize,1.4*(dpi/72.)*fontsize

def font_size(string,space,dpi=96):
    return space/(0.6*(len(string)+2)*(dpi/72.))

class ElementBase:
    def __init__(self,element_name,element_type=None,allow_missing=True,default=None):
        self.element_name = element_name
        self.element_type = element_type
        self.allow_missing = allow_missing
        self.default = default
    def validate(self,input):
        if self.element_name in input:
            if not self.element_type==None:
                if not isinstance(input[self.element_name],self.element_type):
                    return False
            return self._validate(input)
        elif self.allow_missing:
            return True
        else:
            return False
    def _validate(self,input):
        return True
    def extract(self,input):
        if self.element_name in input:
            return input[self.element_name]
        else:
            return self.default

class ColourBase(ElementBase):
    def __init__(self,element_name,default='black'): #any colour you want, providing it's...
        ElementBase.__init__(self,element_name,(str,unicode),True,default)
    def extract(self,input):
        if self.element_name in input:
            c = input[self.element_name]
            if c in matplotlib.colors.cnames:
                return matplotlib.colors.colorConverter.to_rgb(c)
            else:
                match = re.match('^#?([0-9A-Fa-f]{6})$',c) 
                if match:
                    return matplotlib.colors.colorConverter.to_rgb('#%s'%match.group(1))
        elif self.default==None:
            return None
        else:
            return matplotlib.colors.colorConverter.to_rgb(self.default)
       
class IntBase(ElementBase):
    def __init__(self,element_name,min=None,max=None,allow_missing=True,default=None):
        ElementBase.__init__(self,element_name,(int,float),allow_missing,default)
        self.min = min
        self.max = max
    def extract(self,input):
        if self.element_name in input:
            val = int(input[self.element_name])
            if (not self.min==None) and val<self.min:
                val = self.min
            if (not self.max==None) and val>self.max:
                val = self.max
            return val
        else:
            return self.default
        
class FloatBase(ElementBase):
    def __init__(self,element_name,min=None,max=None,allow_missing=True,default=None):
        ElementBase.__init__(self,element_name,(int,float),allow_missing,default)
        self.min = min
        self.max = max
    def extract(self,input):
        if self.element_name in input:
            val = float(input[self.element_name])
            if (not self.min==None) and val<self.min:
                val = self.min
            if (not self.max==None) and val>self.max:
                val = self.max
            return val
        else:
            return self.default
            
class StringBase(ElementBase):
    def __init__(self,element_name,options=None,default=None):
        ElementBase.__init__(self,element_name,(str,unicode),True,default)
        self.options = options
    def extract(self,input):
        if self.element_name in input:
            val = input[self.element_name]
            if (not self.options==None) and (not val in self.options):
                val = self.default
            return val
        else:
            return self.default
        
class FontFamily(StringBase):
    def __init__(self,element_name,default='serif'):
        StringBase.__init__(self,element_name,('serif','sans-serif','monospace'),default)
        
class FontWeight(StringBase):
    def __init__(self,element_name,default='normal'):
        StringBase.__init__(self,element_name,('light','normal','bold'),default)

class ColourMap(StringBase):
    def __init__(self,element_name,default='Accent'):
        ElementBase.__init__(self,element_name,(str,unicode),True,default)
    def extract(self,input):
        if self.element_name in input:
            val = input[self.element_name]
            if matplotlib.cm.get_cmap(val):
                return matplotlib.cm.get_cmap(val)
        return matplotlib.cm.get_cmap(self.default)
               
class FontSize(ElementBase):
    def __init__(self,element_name,default=None):
        ElementBase.__init__(self,element_name,(str,unicode,int),True,default)
    def extract(self,input):
        if self.element_name in input:
            val = input[self.element_name]
            if isinstance(val,int):
                if not (val>0 and val<100):
                    val = self.default
            else:
                if not (val in ('xx-small','x-small','small','medium','large','x-large','xx-large')):
                    val = self.default
            return val
        else:
            return self.default        
        
class ListElementBase(ElementBase):
    def __init__(self,element_name,list_element_type=None,item_validator=None,min_elements=None,max_elements=None,allow_missing=True,default=None):
        ElementBase.__init__(self,element_name,(list,tuple),allow_missing,copy.deepcopy(default))
        self.list_element_type=list_element_type
        self.min_elements = min_elements
        self.max_elements = max_elements
        self.item_validator = item_validator
        if self.item_validator:
            self.item_validator.element_name = 'listitem'
    def validate(self,input):
        if ElementBase.validate(self,input):
            if self.element_name in input:
                if not self.list_element_type==None:
                    if not all([isinstance(item,self.list_element_type) for item in input[self.element_name]]):
                        return False
                if not self.item_validator==None:
                    if not all([self.item_validator.validate({'listitem':item}) for item in input[self.element_name]]):
                        return False
                if not self.min_elements==None:
                    if len(input[self.element_name])<self.min_elements:
                        return False
                if not self.max_elements==None:
                    if len(input[self.element_name])>self.max_elements:
                        return False
            return True
        else:
            return False
    def extract(self,input):
        if self.element_name in input:
            if not self.item_validator==None:
                return [self.item_validator.extract({'listitem':item}) for item in input[self.element_name]]
            else:
                return input[self.element_name]
        else:
            return self.default
        
class DictElementBase(ElementBase):
    def __init__(self,element_name,allow_missing=True,validate_elements=None):
        ElementBase.__init__(self,element_name,dict,allow_missing,None)
        if validate_elements==None:
            self.validate_elements = []
        else:
            self.validate_elements = validate_elements
    def validate(self,input):
        if ElementBase.validate(self,input):
            val = input.get(self.element_name,{})
            for v in self.validate_elements:
                if not v.validate(val):
                    return False
            return True
        else:
            return False
    def extract(self,input):
        if self.element_name in input:
            return dict([(v.element_name,v.extract(input[self.element_name])) for v in self.validate_elements])
        else:
            return dict([(v.element_name,v.extract({})) for v in self.validate_elements])
        
def ThousandWrap(f,char=','):
    re_thousands = re.compile(r'(\d)(\d{3}($|\D))')
    replace = r'\1'+char+r'\2'
    def _inner(*args,**kwargs):
        s = f(*args,**kwargs)
        while True:
            r = re_thousands.sub(replace,s)
            if r==s:
                return r
            else:
                s = r
        return s
    return _inner

def UnitWrap(f,unit):
    def _inner(*args,**kwargs):
        return f(*args,**kwargs)+unit
    return _inner

class TimeFormatter(matplotlib.ticker.ScalarFormatter):
    formats = ((0,'%M:%S'),
               (60,'%H:%M'),
               (3600,'%a %H:%M'),
               (86400,'%m %d'),
               (604800, '%b %d'),
               (2419200,'%d %b %Y'),
               (31557600,'%b %Y'),
               (315576000,'%Y'))
    def __init__(self,span=3600):
        self.span = span
    def __call__(self,val,pos=None):
        t = time.localtime(val)
        for f in formats:
            if f[0]>=self.span:
                return time.strftime(f[1],t)
                
class HexFormatter(matplotlib.ticker.ScalarFormatter):
    def __call__(self,val,pos=None):
        return str(hex(val))           

class SuffixFormatter(matplotlib.ticker.ScalarFormatter):
    suffix = [(1,'')]
    def __call__(self,val,pos=None):
        for s in self.suffix:
            if abs(val)>=100.*s[0]:
                return "%.0f%s"%(val/s[0],s[1])
            if abs(val)>=10.*s[0]:
                return "%.1f%s"%(val/s[0],s[1])
            if abs(val)>=1.*s[0]:
                return "%.2f%s"%(val/s[0],s[1])
        return str(val)

class LongSIFormatter(SuffixFormatter):
    suffix = [(1e18,'exa'),(1e15,'peta'),(1e12,'tera'),(1e9,'giga'),(1e6,'mega'),(1e3,'kilo'),(1,''),(1e-3,'mili'),(1e-6,'micro'),(1e-9,'nano'),(1e-12,'pico'),(1e-15,'femto'),(1e-18,'atto')]

class SIFormatter(SuffixFormatter):
    suffix = [(1e18,'E'),(1e15,'P'),(1e12,'T'),(1e9,'G'),(1e6,'M'),(1e3,'k'),(1,''),(1e-3,'m'),(1e-6,'u'),(1e-9,'n'),(1e-12,'p'),(1e-15,'f'),(1e-18,'a')] 
        
class LongBinFormatter(SuffixFormatter):
    suffix = [(2.**60,'exa'),(2.**50,'peta'),(2.**40,'tera'),(2.**30,'giga'),(2.**20,'mega'),(2.**10,'kilo'),(1,'')]

class BinFormatter(SuffixFormatter):
    suffix = [(2.**60,'E'),(2.**50,'P'),(2.**40,'T'),(2.**30,'G'),(2.**20,'M'),(2.**10,'k'),(1,'')]

class BinaryMaxNLocator(matplotlib.ticker.MaxNLocator):
    def bin_boundaries(self,vmin,vmax):
        scales = (1.,1.5,2.,2.5,3.,4.,5.,6.,8.,10.)
        
        delta = abs(vmax-vmin)
        mean = vmin + 0.5*(vmax-vmin)
        
        bar_width = float(delta)/self._nbins
        bar_magnitude = int(math.log(bar_width,2))
        
        possible = [s*2.**bar_magnitude for s in scales]
        
        best_delta = max(scales)*2.**bar_magnitude
        
        bin_width = 0
        for p in possible:
            if abs(p-bar_width)<best_delta:
                best_delta = abs(p-bar_width)
                bin_width = p
        
        offset = int(vmin/bin_width)
        if vmin<0:
            offset -= 1
        val = offset*bin_width
        result = []
        while val<vmax+bin_width:
            result += [val]
            val += bin_width
        return result
        
class Mixin(object):
    def __init__(self,*args,**kwargs):
        pass
    def validate(self,input):
        return True
    def extract(self,input):
        pass
    def construct(self,*args,**kwargs):
        pass
    def predata(self,*args,**kwargs):
        pass
    def data(self,*args,**kwargs):
        pass
    def postdata(self,*args,**kwargs):
        pass
    def finalise(self,*args,**kwargs):
        pass

class FigureMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [IntBase('height',min=1,max=5000,default=600),
                           IntBase('width',min=1,max=5000,default=800),
                           FloatBase('dpi',min=1,max=300,default=96.)] 
        super(FigureMixin,self).__init__(*args,**kwargs)
    def construct(self,*args,**kwargs):
        self.figure = matplotlib.pyplot.figure(figsize=(self.props.width/self.props.dpi,self.props.height/self.props.dpi),
                                               dpi=self.props.dpi)
        super(FigureMixin,self).construct(*args,**kwargs)

class TitleMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ElementBase('notitle',bool,default=False),
                  ElementBase('title',(str,unicode),default=''),
                  FontSize('title_size',default=14),
                  FontSize('subtitle_size',default=12),
                  FontWeight('title_weight',default='bold'),
                  FontWeight('subtitle_weight',default='normal'),
                  FontFamily('title_font',default='serif'),
                  FontFamily('subtitle_font',default='serif'),
                  ColourBase('title_colour',default='black'),
                  ColourBase('subtitle_colour',default='black'),
                  IntBase('padding_top',min=0,default=kwargs.get('Padding_Top',50)),
                  IntBase('padding_left',min=0,default=kwargs.get('Padding_Left',70)),
                  IntBase('padding_right',min=0,default=kwargs.get('Padding_Right',30)),
                  IntBase('padding_bottom',min=0,default=kwargs.get('Padding_Bottom',50)),
                  IntBase('linepadding',min=0,default=10)]    
        super(TitleMixin,self).__init__(*args,**kwargs)
    def construct(self,*args,**kwargs):
        self.props.topbound = self.props.height-self.props.padding_top
        if (not self.props.notitle) and len(self.props.title)>0:
            title = self.props.title.split('\n')
            
            tx,ty = text_size(title[0],self.props.title_size,self.props.dpi)
            h = self.props.height
            ch = h-self.props.linepadding
            self.figure.text(0.5,(ch-(ty*0.5))/h,title[0],
                        color=self.props.title_colour,
                        family=self.props.title_font,
                        weight=self.props.title_weight,
                        size=self.props.title_size,
                        ha='center',
                        va='center')
            ch -= ty
            ch -= self.props.linepadding
            
            for subtitle in title[1:]:
                tx,ty = text_size(subtitle,self.props.subtitle_size,self.props.dpi)
                self.figure.text(0.5,(ch-(ty*0.5))/h,
                            subtitle,color=self.props.subtitle_colour,
                            family=self.props.subtitle_font,
                            weight=self.props.subtitle_weight,
                            size=self.props.subtitle_size,
                            ha='center',
                            va='center')
                ch -= ty
                ch -= self.props.linepadding
                
            self.props.topbound = ch
        super(TitleMixin,self).construct(*args,**kwargs)
    

class FigAxesMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [StringBase('projection',('aitoff','hammer','lambert','mollweide','polar'),kwargs.get('Axes_Projection','rectilinear')),
                  ElementBase('square',bool,default=kwargs.get('Axes_Square',False)),
                  IntBase('padding_top',min=0,default=kwargs.get('Padding_Top',50)),
                  IntBase('padding_left',min=0,default=kwargs.get('Padding_Left',70)),
                  IntBase('padding_right',min=0,default=kwargs.get('Padding_Right',30)),
                  IntBase('padding_bottom',min=0,default=kwargs.get('Padding_Bottom',50))]    
        super(FigAxesMixin,self).__init__(*args,**kwargs)
    def construct(self,*args,**kwargs):
        w,h = self.props.width,self.props.height
        topbound = self.props.get('topbound',h)
        p_top,p_left,p_right,p_bottom = self.props.padding_top,self.props.padding_left,self.props.padding_right,self.props.padding_bottom
        square = self.props.square
        projection = self.props.projection
        if h-topbound<p_top:
            topbound = h-p_top
        
        avail_width = w - p_left - p_right
        avail_height = topbound - p_bottom
        
        self.props.avail_width = avail_width
        self.props.avail_height = avail_height
        
        if square:
            max_dim = min(avail_width,avail_height)
            left = p_left + (0.5*avail_width - 0.5*max_dim)
            bottom = p_bottom + (0.5*avail_height - 0.5*max_dim)
            self.figure.add_axes((float(left)/w,float(bottom)/h,float(max_dim)/w,float(max_dim)/h),projection=projection)
        else:
            self.figure.add_axes((float(p_left)/w,float(p_bottom)/h,float(avail_width)/w,float(avail_height)/h),projection=projection)
        super(FigAxesMixin,self).construct(*args,**kwargs)
        
class StyleMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ColourBase('background',default=None),
                  ColourMap('colourmap',default=None),
                  ElementBase('gridlines',bool,default=True)]
        super(StyleMixin,self).__init__(*args,**kwargs)    
    def construct(self,*args,**kwargs):
        if not self.props.background==None:
            self.figure.set_facecolor(self.props.background)
        if self.props.gridlines:
            self.figure.gca().grid()
        super(StyleMixin,self).construct(*args,**kwargs)

"""
A note on axis mixins. The intention was that these would be inherit
from simpler axis types, eg NumericAxis->BinnedNumericAxis->XBinnedNumericAxis.
However, consider the case where two axes are numeric. The {X,Y}Axis classes
contain nothing, but have a common subclass. Using super() execution,
the inherited class that actually does something (NumericAxis, BinnedNumericAxis)
will only get executed once, with whatever self.axis was set last.

So, first thought, we have a function that takes a final class, and
returns a new type which inherits from uniquified versions of all non
trivial superclasses. Ie
BinnedNumericAxis->NumericAxis->Mixin
Uniquify(BinnedNumericAxis,'X') -> XBinnedNumericAxis (bases XNumericAxis,Mixin)

Which is fine, but I can't find any way of modifying the functions within the
original class (eg __init__) so that super() execution uses the correct new class
name instead of the original one. Unless some way of determining the current class
a virtual function is from turns up (function_object.im_class would help but I can't
think how to get function_object, except perhaps stack inspection). I also can't use
__name mangling because the name mangling is done at code parsing time - ie, while
still part of NumericAxis instead of XNumericAxis.

There might be a metaclass solution here, but again I can't think of it.

See PEP 3130 for a discussion of this issue, also
http://groups.google.com/group/comp.lang.python/browse_frm/thread/a6010c7494871bb1/62a2da68961caeb6?lnk=gst&q=simionato+challenge&rnum=1&hl=en#62a2da68961caeb6

It looks like by creating a new code object, and a new function object,
and search-replacing names in the string table I can do it though.

This is such a bad idea...
"""
def UniqueAxis(axisclass, axis):
    prefix = axis[0].upper()
    name = prefix+axisclass.__name__
    bases = list(axisclass.__bases__)
    for i,base in enumerate(bases[:]):
        if 'Axis' in base.__name__:
            newname = prefix+base.__name__
            if newname in globals():
                bases[i] = globals()[newname]
            else:
                bases[i] = UniqueAxis(base, axis)
    attrs = copy.deepcopy(dict(axisclass.__dict__))
    newtype = type(name,tuple(bases),attrs)
    setattr(newtype,'_%s__axis'%name,axis)
    for n,v in newtype.__dict__.items():
        if type(v)==types.FunctionType:
            newcode = new.code(v.func_code.co_argcount,
                                   v.func_code.co_nlocals,
                                   v.func_code.co_stacksize,
                                   v.func_code.co_flags,
                                   v.func_code.co_code,
                                   v.func_code.co_consts,
                                   tuple([n.replace(axisclass.__name__,name) for n in v.func_code.co_names]),
                                   v.func_code.co_varnames,
                                   v.func_code.co_filename,
                                   v.func_code.co_name,
                                   v.func_code.co_firstlineno,
                                   v.func_code.co_lnotab)
            newfunc = new.function(newcode,v.func_globals,v.func_name,v.func_defaults)
            setattr(newtype,n,newfunc)
    
    return newtype

def axis_format(axis,data):
    format = data.get('format','num')
    if format=='si':
        axis.set_major_formatter(SIFormatter())
    elif data['format']=='time':
         if not data['timeformat']==None:
             axis.set_major_formatter(TimeFormatter(data['timeformat']))
         else:
             axis.set_major_formatter(TimeFormatter())
             axis.set_major_locator(TimeLocator())
    elif data['format']=='binary':
        axis.set_major_formatter(BinFormatter())
        axis.set_major_locator(BinaryMaxNLocator())
    elif data['format']=='hex':
        axis.set_major_formatter(HexFormatter())
        axis.set_major_locator(BinaryMaxNLocator())

def numeric_bins(data):
    min = data.get('min',None)
    max = data.get('max',None)
    width = data.get('width',None)
    bins = data.get('bins',None)
    log = data.get('log',False)
    logbase = float(data.get('logbase',10))
    
    if width!=None and width<=0:
        return None,None
    if bins!=None and bins<=0:
        return None,None
    if min!=None and max!=None and width!=None:
        if log:
            if min<=0 or max<=0 or width<=0:
                return None, None
            else:
                bins = int(abs(math.log(min,logbase)-math.log(max,logbase))/width)
                edges = [logbase**(math.log(min,logbase)+i*width) for i in range(bins+1)]
                return bins,edges    
        else:
            bins = int(float(max-min)/width)
            edges = [min+width*i for i in range(bins+1)]
            return bins,edges
    elif min!=None and width!=None and bins!=None:
        if log:
            if min<=0 or width<=0 or bins<=0:
                return None,None
            else:
                edges = [logbase**(math.log(min,logbase)+i*width) for i in range(bins+1)]
                return bins,edges
        else:
            edges = [min+width*i for i in range(bins+1)]
            return bins,edges
    return None,None
                
class NumericAxisMixin(Mixin):  
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,True,[StringBase('label',None,default=''),
                                                     FloatBase('min',default=None),
                                                     FloatBase('max',default=None),
                                                     ElementBase('log',bool,default=False),
                                                     FloatBase('logbase',min=1,default=10),
                                                     StringBase('timeformat',None,default=None),
                                                     StringBase('format',('num','time','binary','si','hex'),default='num')])]
        super(NumericAxisMixin,self).__init__(*args,**kwargs)
    def predata(self,*args,**kwargs):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        axis_format(axis,data)
        
        if self.__axis=='xaxis':
            axes.set_xlabel(data['label'])
        elif self.__axis=='yaxis':
            axes.set_ylabel(data['label'])
        
        if data['log']:
            if self.__axis=='xaxis':
                axes.set_xscale('log',basex=data['logbase'])
            elif self.__axis=='yaxis':
                axes.set_yscale('log',basey=data['logbase'])
            setattr(self.props,'log_%s'%self.__axis[0].lower(),True)
        else:
            setattr(self.props,'log_%s'%self.__axis[0].lower(),False)
        super(NumericAxisMixin,self).predata(*args,**kwargs)
    
    def postdata(self,*args,**kwargs):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        if data['min'] or data['max']:
            axis.set_view_interval(data['min'],data['max'])
        
        super(NumericAxisMixin,self).postdata(*args,**kwargs)

XNumericAxisMixin = UniqueAxis(NumericAxisMixin,'xaxis')
YNumericAxisMixin = UniqueAxis(NumericAxisMixin,'yaxis')
        
        
class BinnedNumericAxisMixin(NumericAxisMixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,False,[FloatBase('min',default=0),
                                                     FloatBase('max',default=1),
                                                     FloatBase('width',min=0,default=1)])]
        super(BinnedNumericAxisMixin,self).__init__(*args,**kwargs)
    def predata(self,*args,**kwargs):
        data = self.props.get(self.__axis)
        
        bins,edges = numeric_bins(data)
        data['bins']=bins
        data['edges']=edges
        
        super(BinnedNumericAxisMixin,self).predata(*args,**kwargs)

XBinnedNumericAxisMixin = UniqueAxis(BinnedNumericAxisMixin,'xaxis')
YBinnedNumericAxisMixin = UniqueAxis(BinnedNumericAxisMixin,'yaxis')

class AnyBinnedAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,False,[StringBase('label',None,default=''),
                                                               FloatBase('min',default=None),
                                                               FloatBase('max',default=None),
                                                               FloatBase('width',min=0,default=None),
                                                               StringBase('timeformat',None,default=None),
                                                               StringBase('format',('num','time','binary','si','hex'),default='num'),
                                                               ListElementBase('labels',(str,unicode),default=None)])]
        super(AnyBinnedAxisMixin,self).__init__(*args,**kwargs)
    def predata(self,*args,**kwargs):       
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        if not data['labels']==None:
            data['bins'] = len(data['labels'])
            data['edges'] = range(len(data['labels'])+1)
            axis.set_ticklabels(data['labels'])
            axis.set_ticks([i+0.5 for i in range(len(data['labels']))])
        else:
            bins,edges = numeric_bins(data)
            data['bins']=bins
            data['edges']=edges
        if self.__axis=='xaxis':
            axes.set_xlabel(data['label'])
        elif self.__axis=='yaxis':
            axes.set_ylabel(data['label'])
        super(AnyBinnedAxisMixin,self).predata(*args,**kwargs)

XAnyBinnedAxisMixin = UniqueAxis(AnyBinnedAxisMixin,'xaxis')
YAnyBinnedAxisMixin = UniqueAxis(AnyBinnedAxisMixin,'yaxis')
        
class LabelledAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,False,[StringBase('label',None,default=''),
                                                     ListElementBase('labels',(str,unicode),min_elements=1,default=('default',))])]
        super(LabelledAxisMixin,self).__init__(*args,**kwargs)   
    def predata(self,*args,**kwargs):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        if self.__axis=='xaxis':
            axes.set_xlabel(data['label'])
        elif self.__axis=='yaxis':
            axes.set_ylabel(data['label'])
        axis.set_ticklabels(data['labels'])
        axis.set_ticks([i+0.5 for i in range(len(data['labels']))])
        
        data['bins'] = len(data['labels'])
        data['edges'] = range(len(data['labels'])+1)
        
        super(LabelledAxisMixin,self).predata(*args,**kwargs)

XLabelledAxisMixin = UniqueAxis(LabelledAxisMixin,'xaxis')
YLabelledAxisMixin = UniqueAxis(LabelledAxisMixin,'yaxis')
    
class AutoLabelledAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,True,[StringBase('label',None,default='')])]
        super(AutoLabelledAxisMixin,self).__init__(*args,**kwargs)
    def predata(self,*args,**kwargs):       
        axes = self.figure.gca()
        data = self.props.get(self.__axis)
        if self.__axis=='xaxis':
            axes.set_xlabel(data['label'])
        elif self.__axis=='yaxis':
            axes.set_ylabel(data['label'])
        super(AutoLabelledAxisMixin,self).predata(*args,**kwargs)

XAutoLabelledAxisMixin = UniqueAxis(AutoLabelledAxisMixin,'xaxis')
YAutoLabelledAxisMixin = UniqueAxis(AutoLabelledAxisMixin,'yaxis')
            
class BinnedNumericSeriesMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ListElementBase('series',dict,DictElementBase('listitem',False,[StringBase('label',None,default=''),ListElementBase('values',(int,float),allow_missing=False),ColourBase('colour',default=None)]),allow_missing=False)]
        self.__datamode = kwargs.get('BinnedNumericSeries_DataMode','bin')
        self.__logmode = kwargs.get('BinnedNumericSeries_LogMode','clean')
        self.__logsrc = kwargs.get('BinnedNumericSeries_LogSrc','log_y')
        self.__binsrc = kwargs.get('BinnedNumericSeries_BinSrc','xaxis')
        
        super(BinnedNumericSeriesMixin,self).__init__(*args,**kwargs)
    def data(self,*args,**kwargs):
        cmap = self.props.colourmap
        if self.__binsrc!=None:
            xbins = self.props.get(self.__binsrc,{}).get('bins',0)
            if xbins==None:
                super(BinnedNumericSeriesMixin,self).data(*args,**kwargs)
                return
        else:
            xbins = None
        if self.__datamode == 'edge':
            xbins += 1
            
        log_enabled = self.props.get(self.__logsrc,False)
        
        for i,series in enumerate(self.props.series):
            if series['colour']==None:
                series['colour']=cmap(float(i)/len(self.props['series']))
            if xbins!=None:
                if len(series['values'])>xbins:
                    series['values'] = series['values'][:xbins]
                elif len(series['values'])<xbins:
                    series['values'] = series['values'] + [0]*(xbins-len(series['values']))
            if log_enabled:
                cls = CleanLogSeries(series['values'])
                if self.__logmode=='first_nonzero':
                    if i==0:
                        series['values']=cls.remove_negorzero(cls.minpos)
                    else:
                        series['values']=cls.remove_negative()
                elif self.__logmode=='all_nonzero':
                    series['values']=cls.remove_negorzero(cls.minpos)
                series['logmin'] = cls.minpos
                series['logmax'] = cls.maxpos
                series['logmax_round'] = cls.roundmax()
            else:
                series['logmin'] = 0
                series['logmax'] = 0
                series['logmax_round'] = 0
            series['min'] = min(series['values'])
            series['max'] = max(series['values'])
            
        super(BinnedNumericSeriesMixin,self).data(*args,**kwargs)
    
class LabelledSeriesMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ListElementBase('series',dict,DictElementBase('listitem',False,[StringBase('label',None,default=''),FloatBase('value',allow_missing=False),ColourBase('colour',default=None),ElementBase('explode',(int,float),default=0)]),allow_missing=False)]
        super(LabelledSeriesMixin,self).__init__(*args,**kwargs)
    def data(self,*args,**kwargs):
        cmap = self.props.colourmap
        for i,series in enumerate(self.props.series):
            if series['colour']==None:
                series['colour']=cmap(float(i)/len(self.props.series))
        super(LabelledSeriesMixin,self).data(*args,**kwargs)
        
class ArrayMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ListElementBase('data',(list,tuple),ListElementBase('listitem',(int,float),FloatBase('listitem',min=0.,max=1.),allow_missing=False),allow_missing=False)]
        self.__min = kwargs.get('Array_Min',None)
        self.__max = kwargs.get('Array_Max',None)
        self.__rowlen = kwargs.get('Array_RowLen',None)
        super(ArrayMixin,self).__init__(*args,**kwargs)
    def validate(self,input):
        lengths = set([len(line) for line in input['data']])
        if not len(lengths)==1:
            return False
        if self.__rowlen!=None and not list(lengths)[0]==self.__rowlen:
            return False
        return super(ArrayMixin,self).validate(input)
    def predata(self,*args,**kwargs):
        if self.__min!=None or self.__max!=None:
            for row in self.props.data:
                for i,item in enumerate(row[:]):
                    if self.__min!=None and item<self.__min:
                        row[i]=self.__min
                    if self.__max!=None and item>self.__max:
                        row[i]=self.__max
        super(ArrayMixin,self).predata(*args,**kwargs)

class Props:
    def get(self,name,default=None):
        if not hasattr(self,name):
            return default
        return getattr(self,name)          
    
class CleanLogSeries:
    def __init__(self,series):
        self.series = series
        self.min = min(series)
        self.max = max(series)
        self.positive = filter(lambda x: x>0,series)
        if len(self.positive)>0:
            self.minpos = min(self.positive)
            self.maxpos = max(self.positive)
            self.valid = True
        else:
            self.valid = False
            self.minpos = 0
            self.maxpos = 0
    def remove_negative(self):
        result = []
        for i in self.series:
            if i<0:
                result += [0]
            else:
                result += [i]
        return result
    def remove_negorzero(self,replace):
        result = []
        for i in self.series:
            if i<=0:
                result += [replace]
            else:
                result += [i]
        return result
    def roundmin(self):
        if self.minpos:
            return 10**(int(math.log10(self.minpos)))
        else:
            return None
    def roundmax(self):
        if self.maxpos:
            l = math.log10(self.maxpos)
            if abs(int(l)-l)<0.001:
                return 10**l
            else:
                return 10**(l+1)
        else:
            return None
             
    
    
        
    
        

