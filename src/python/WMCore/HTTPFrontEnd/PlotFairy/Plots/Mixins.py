from Validators import *
from Utils import *

import matplotlib.ticker

class Mixin(object):
    def __init__(self,*args,**kwargs):
        pass

class FigureMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [IntBase('height',min=1,max=5000,default=600,doc_user="Canvas height in pixels."),
                           IntBase('width',min=1,max=5000,default=800,doc_user="Canvas width in pixels."),
                           FloatBase('dpi',min=1,max=300,default=100.,doc_user="Canvas DPI.")] 
        super(FigureMixin,self).__init__(*args,**kwargs)
    def construct(self,*args,**kwargs):
        self.figure = matplotlib.pyplot.figure(figsize=(self.props.width/self.props.dpi,self.props.height/self.props.dpi),
                                               dpi=self.props.dpi)
        #super(FigureMixin,self).construct(*args,**kwargs)

class TitleMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ElementBase('notitle',bool,default=False,doc_user="Prevent drawing and allocating space for a title."),
                  ElementBase('title',(str,unicode),default='',doc_user="Title of the plot. Blank for none. Anything after the first newline will be rendered as `subtitle'."),
                  FontSize('title_size',default=14,doc_user="Font size for title."),
                  FontSize('subtitle_size',default=12,doc_user="Font size for subtitle."),
                  FontWeight('title_weight',default='bold',doc_user="Text style for title."),
                  FontWeight('subtitle_weight',default='normal',doc_user="Text style for subtitle."),
                  FontFamily('title_font',default='serif',doc_user="Font family for title."),
                  FontFamily('subtitle_font',default='serif',doc_user="Font family for subtitle."),
                  ColourBase('title_colour',default='black',doc_user="Text colour for title."),
                  ColourBase('subtitle_colour',default='black',doc_user="Text colour for subtitle."),
                  IntBase('padding_top',min=0,default=kwargs.get('Padding_Top',50),doc_user="Padding in pixels at plot top. Title may go over this area."),
                  IntBase('padding_left',min=0,default=kwargs.get('Padding_Left',70),doc_user="Padding in pixels at plot left."),
                  IntBase('padding_right',min=0,default=kwargs.get('Padding_Right',30),doc_user="Padding in pixels at plot right."),
                  IntBase('padding_bottom',min=0,default=kwargs.get('Padding_Bottom',50),doc_user="Padding in pixels at plot bottom."),
                  IntBase('linepadding',min=0,default=10,doc_user="Padding in pixels between lines in the title.")]    
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
        #super(TitleMixin,self).construct(*args,**kwargs)
    

class FigAxesMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [StringBase('projection',('aitoff','hammer','lambert','mollweide','polar'),kwargs.get('Axes_Projection','rectilinear'),doc_user="Projection algorithm to use. Changing this may cause undesirable side effects."),
                  ElementBase('square',bool,default=kwargs.get('Axes_Square',False),doc_user="Force the plot area to be square, leaving necessary margins.")]
                  #IntBase('padding_top',min=0,default=kwargs.get('Padding_Top',50)),
                  #IntBase('padding_left',min=0,default=kwargs.get('Padding_Left',70)),
                  #IntBase('padding_right',min=0,default=kwargs.get('Padding_Right',30)),
                  #IntBase('padding_bottom',min=0,default=kwargs.get('Padding_Bottom',50))]    
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
            self.props.axes_left = left
            self.props.axes_bottom = bottom
        else:
            left = float(p_left)/w
            bottom = float(p_bottom)/w
            self.figure.add_axes((left,bottom,float(avail_width)/w,float(avail_height)/h),projection=projection)
            self.props.axes_left = left
            self.props.axes_bottom = bottom
        #super(FigAxesMixin,self).construct(*args,**kwargs)
        
class StyleMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ColourBase('background',default=None,doc_user="Background colour for the canvas."),
                  ColourMap('colourmap',default=None,doc_user="Colourmap to use for items without preset colours."),
                  ElementBase('gridlines',bool,default=True,doc_user="Whether to show gridlines.")]
        super(StyleMixin,self).__init__(*args,**kwargs)    
    def construct(self,*args,**kwargs):
        if not self.props.background==None:
            self.figure.set_facecolor(self.props.background)
        if self.props.gridlines:
            self.figure.gca().grid()
        #super(StyleMixin,self).construct(*args,**kwargs)

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
        #print n
        if type(v)==types.FunctionType:
            #print 'rebuilding',n
            newcode = new.code(v.func_code.co_argcount,
                                   v.func_code.co_nlocals,
                                   v.func_code.co_stacksize,
                                   v.func_code.co_flags,
                                   v.func_code.co_code,
                                   v.func_code.co_consts,
                                   tuple([nn.replace(axisclass.__name__,name) for nn in v.func_code.co_names]),
                                   v.func_code.co_varnames,
                                   v.func_code.co_filename,
                                   v.func_code.co_name,
                                   v.func_code.co_firstlineno,
                                   v.func_code.co_lnotab)
            newfunc = new.function(newcode,v.func_globals,v.func_name,v.func_defaults)
            setattr(newtype,n,newfunc)
            #print 'rebuilt',n,newfunc.func_code.co_names,id(newfunc)
    return newtype

def axis_format(axis,data):
    format = data.get('format','num')
    if format=='si':
        axis.set_major_formatter(SIFormatter())
    elif format=='time':
         if not data.get('timeformat',None)==None:
             axis.set_major_formatter(TimeFormatter(data.get('timeformat','hour')))
         else:
             axis.set_major_formatter(TimeFormatter())
             #axis.set_major_locator(TimeLocator()) not yet written...
    elif format=='binary':
        axis.set_major_formatter(BinFormatter())
        axis.set_major_locator(BinaryMaxNLocator())
    elif format=='hex':
        axis.set_major_formatter(HexFormatter())
        axis.set_major_locator(BinaryMaxNLocator())
    elif format=='num':
        pass
    else:
        raise ValueError, "unknown axis format '%s'"%format

def numeric_bins(data):
    min = data.get('min',None)
    max = data.get('max',None)
    width = data.get('width',None)
    bins = data.get('bins',None)
    log = data.get('log',False)
    logbase = float(data.get('logbase',10))
    
    if width!=None and width<=0:
        raise ValueError, "axis 'width' defined and <=0"
    if bins!=None and bins<=0:
        raise ValueError, "axis 'bins' defined and <=0"
    if log and min!=None and min<=0:
        raise ValueError, "log axis has 'min'<=0"
    if log and max!=None and max<=0:
        raise ValueError, "log axis has 'max'<=0"
    if min!=None and max!=None and min>max:
        raise ValueError, "axis 'min'>'max'"
    
    if min!=None and max!=None and width!=None:
        if log:
            bins = int(abs(math.log(min,logbase)-math.log(max,logbase))/width)
        else:
            bins = int(float(max-min)/width)
    elif min!=None and width!=None and bins!=None:
        pass
    elif min!=None and max!=None and bins!=None:
        if log:
            width = math.log(max,logbase)-math.log(min,logbase)
        else:
            width = max-min
    elif max!=None and bins!=None and width!=None:
        if log:
            min = logbase**(math.log(max,logbase)-bins*width)
        else:
            min = max - bins*width
    else:
        raise ValueError, "axis requires at least 3 of 'min','max','width','bins'"
    
    if log:
        return bins, [logbase**(math.log(min,logbase)+i*width) for i in range(bins+1)]
    else:
        return bins, [min+width*i for i in range(bins+1)]
    
    
                
def fix_y_axis_labels(axes,props,text,labels):    
    ylabel_width = text_size('',axes.get_yaxis().get_label().get_size())[1]
    space = props.axes_left*props.width - ylabel_width - props.linepadding
            
    sizes = [font_size(t,space) for t in text]
    minsize = min(sizes)
    if minsize<6:
        space = text_size(text[sizes.index(minsize)],6)[0]
        req_space = space + ylabel_width + props.linepadding
                
        props.avail_width = (props.avail_width - (req_space-props.axes_left*props.width))
        props.axes_left = req_space/props.width
        axes.set_position([props.axes_left,props.axes_bottom,float(props.avail_width)/props.width,float(props.avail_height)/props.height])
        sizes = [font_size(t,space) for t in text]
    for s,l in zip(sizes,labels):
        l.set_size(s)
class NumericAxisMixin(Mixin):  
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,True,[StringBase('label',None,default='',doc_user="Axis label."),
                                                     FloatBase('min',default=None,doc_user="Minimum view-limit for axis."),
                                                     FloatBase('max',default=None,doc_user="Maximum view-limit for axis."),
                                                     ElementBase('log',bool,default=False,doc_user="Use logarithmic scale."),
                                                     FloatBase('logbase',min=1,default=10,doc_user="Exponent of log scale, if enabled."),
                                                     StringBase('timeformat',default=None,doc_user="Time format to use, if format=time. Can either be a strftime() format string or a preset from ('second','minute','hour','day','week','month','year','decade')."),
                                                     StringBase('format',('num','time','binary','si','hex'),default='num',doc_user="Formatting to use for numbers on this axis.")])]
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
        
    def postdata(self,*args,**kwargs):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        if data['min'] or data['max']:
            axis.set_view_interval(data['min'],data['max'])
        
        if self.__axis=='yaxis' and self.props.yaxis['format']=='time':
            formatter = axes.get_yaxis().get_major_formatter()
            text = [str(formatter(i)) for i in axes.get_yticks()]
            labels = axes.get_yticklabels()
            
            fix_y_axis_labels(axes,self.props,text,labels)
            
XNumericAxisMixin = UniqueAxis(NumericAxisMixin,'xaxis')
YNumericAxisMixin = UniqueAxis(NumericAxisMixin,'yaxis')
        
        
class BinnedNumericAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.__binsrc = kwargs.get('BinnedNumericAxis_defaultbinsrc','series')
        self.__allowdefault = kwargs.get('BinnedNumericAxis_allowdefault',False)
        subvalidators = [
                         FloatBase('min',default=None,doc_user="Lower edge of first bin."),
                         FloatBase('max',default=None,doc_user="Lower edge of last bin."),
                         FloatBase('width',default=None,doc_user="Width of each bin. If logarithmic, this is the width of each bin after log conversion."),
                         IntBase('bins',default=None,doc_user="Number of bins on this axis. Data will be truncated or padded to reflect this number."),
                         StringBase('label',default='',doc_user="Axis label."),
                         ElementBase('log',bool,default=False,doc_user="Use logarithmic scale."),
                         FloatBase('logbase',min=0,default=10,doc_user="Exponent of log scale, if enabled."),
                         StringBase('timeformat',default=None,doc_user="Time format to use, if format=time. Can either be a strftime() format string or a preset from ('second','minute','hour','day','week','month','year','decade')."),
                         StringBase('format',('num','time','binary','si','hex'),default='num',doc_user="Formatting to use for numbers on this axis."),
                         IntBase('ticks',min=0,default=6,doc_user="How many labels to place on the axis (approximately).")
                         ]
        self.validators += [DictElementBase(self.__axis,True,subvalidators,doc_user="For a binned axis, at least three of 'min', 'max', 'width' and 'bins' must be specified.")]
        super(BinnedNumericAxisMixin,self).__init__(*args,**kwargs)
    def validate(self,input):
        if not self.__allowdefault:
            if not self.__axis in input:
                return "'%s' not found in input, required"%self.__axis
            else:
                count = sum([1 if s in input[self.__axis] else 0 for s in ('min','max','width','bins')])
                if count < 3:
                    return "%s requires at least 3 of (min, max, width, bins)"%self.__axis
        return True
    
    def predata(self,*args,**kwargs):
        data = self.props.get(self.__axis)
        bins = 0
        edges = []
        if self.__allowdefault:  
            try:
                bins,edges = numeric_bins(data)
            except:
                binsrc = self.props.get(self.__binsrc,[])
                bins = max([len(s['values']) for s in binsrc])
                edges = range(bins+1)
                
        else:
            bins,edges = numeric_bins(data)
        data['bins']=bins
        data['edges']=edges
        
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        axis_format(axis,data)
        axis.set_major_locator(matplotlib.ticker.FixedLocator(edges,data['ticks']))
        
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
        
    def postdata(self,*args,**kwargs):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        if data['min'] or data['max']:
            axis.set_view_interval(data['min'],data['max'])
            
        if self.__axis=='yaxis' and data['format']=='time':
            formatter = axes.get_yaxis().get_major_formatter()
            text = [str(formatter(i)) for i in axes.get_yticks()]
            labels = axes.get_yticklabels()
            fix_y_axis_labels(axes,self.props,text,labels)
                    
XBinnedNumericAxisMixin = UniqueAxis(BinnedNumericAxisMixin,'xaxis')
YBinnedNumericAxisMixin = UniqueAxis(BinnedNumericAxisMixin,'yaxis')

class AnyBinnedAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,False,[StringBase('label',None,default='',doc_user="Axis label."),
                                                               FloatBase('min',default=None,doc_user="Lower edge of first bin."),
                                                               FloatBase('max',default=None,doc_user="Lower edge of last bin."),
                                                               FloatBase('width',min=0,default=None,doc_user="Width of bins on this axis."),
                                                               IntBase('bins',min=0,default=None,doc_user="Number of bins on this axis. Data will be truncated or padded to reflect this number."),
                                                               StringBase('timeformat',None,default=None,doc_user="Time format to use, if format=time. Can either be a strftime() format string or a preset from ('second','minute','hour','day','week','month','year','decade')."),
                                                               StringBase('format',('num','time','binary','si','hex'),default='num',doc_user="Formatting to use for numbers on this axis."),
                                                               IntBase('ticks',min=0,default=6,doc_user="How many labels to place on the axis (approximately)."),
                                                               ListElementBase('labels',basestring,default=None,doc_user="List of labels for this axis.")],doc_user="Must either specify a list of labels or at least 3 of 'min', 'max', 'width', 'bins'.")]
        super(AnyBinnedAxisMixin,self).__init__(*args,**kwargs)
    def predata(self,*args,**kwargs):       
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        axis_format(axis,data)
        
        if not data['labels']==None:
            data['bins'] = len(data['labels'])
            data['edges'] = range(len(data['labels'])+1)
            axis.set_ticklabels(data['labels'])
            axis.set_ticks([i+0.5 for i in range(len(data['labels']))])
        else:
            bins,edges = numeric_bins(data)
            data['bins']=bins
            data['edges']=edges
            axis.set_major_locator(matplotlib.ticker.FixedLocator(edges,data['ticks']))
        if self.__axis=='xaxis':
            axes.set_xlabel(data['label'])
        elif self.__axis=='yaxis':
            axes.set_ylabel(data['label'])
        #super(AnyBinnedAxisMixin,self).predata(*args,**kwargs)
        
    def postdata(self,*args,**kwargs):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        if data['min'] or data['max']:
            axis.set_view_interval(data['min'],data['max'])
            
        if self.__axis=='yaxis':
            if data['format']=='time':
                formatter = axes.get_yaxis().get_major_formatter()
                text = [str(formatter(i)) for i in axes.get_yticks()]
                labels = axes.get_yticklabels()
            elif data['labels']!=None:
                text = data['labels']
                labels = axes.get_yticklabels()
            else:
                return
            fix_y_axis_labels(axes,self.props,text,labels)    

XAnyBinnedAxisMixin = UniqueAxis(AnyBinnedAxisMixin,'xaxis')
YAnyBinnedAxisMixin = UniqueAxis(AnyBinnedAxisMixin,'yaxis')
        
class LabelledAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,False,[StringBase('label',None,default='',doc_user="Axis label."),
                                                     ListElementBase('labels',basestring,min_elements=1,default=('default',),doc_user="List of labels for this axis.")])]
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
        
    def postdata(self):
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        
        if self.__axis=='yaxis':
            text = [str(formatter(i)) for i in axes.get_yticks()]
            labels = data['labels']
            fix_y_axis_labels(axes,self.props,text,labels)

XLabelledAxisMixin = UniqueAxis(LabelledAxisMixin,'xaxis')
YLabelledAxisMixin = UniqueAxis(LabelledAxisMixin,'yaxis')
    
class AutoLabelledAxisMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [DictElementBase(self.__axis,True,
                                            [StringBase('label',None,default='',doc_user="Axis label."),
                                             ElementBase('log',bool,default=False,doc_user="Use logarithmic scale."),
                                             FloatBase('logbase',min=0,default=10,doc_user="Exponent of log scale, if enabled."),
                                             StringBase('timeformat',default=None,doc_user="Time format to use, if format=time. Can either be a strftime() format string or a preset from ('second','minute','hour','day','week','month','year','decade')."),
                                             StringBase('format',('num','time','binary','si','hex'),default='num',doc_user="Formatting to use for numbers on this axis."),   
                                             ])
                            ]
        #print 'AutoLabelledAxisMixin::__init__',self.__class__,self.__dict__.keys()
        super(AutoLabelledAxisMixin,self).__init__(*args,**kwargs)
    def predata(self,*args,**kwargs):
        #print 'AutoLabelledAxisMixin::predata',self.__class__,self.__dict__.keys()
        axes = self.figure.gca()
        axis = getattr(axes,self.__axis)
        data = self.props.get(self.__axis)
        if self.__axis=='xaxis':
            axes.set_xlabel(data['label'])
        elif self.__axis=='yaxis':
            axes.set_ylabel(data['label'])
        data['bins']=0
        data['edges']=[]
        
        axis_format(axis,data)
        
        if data['log']:
            if self.__axis=='xaxis':
                axes.set_xscale('log',basex=data['logbase'])
            elif self.__axis=='yaxis':
                axes.set_yscale('log',basey=data['logbase'])
            setattr(self.props,'log_%s'%self.__axis[0].lower(),True)
        else:
            setattr(self.props,'log_%s'%self.__axis[0].lower(),False)

XAutoLabelledAxisMixin = UniqueAxis(AutoLabelledAxisMixin,'xaxis')
YAutoLabelledAxisMixin = UniqueAxis(AutoLabelledAxisMixin,'yaxis')

class BinnedNumericSeriesMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ListElementBase('series',dict,DictElementBase('listitem',False,[
                                                                                            StringBase('label',None,default='',doc_user="Series label."),
                                                                                            ListElementBase('values',(int,float),allow_missing=False,doc_user="List of series values."),
                                                                                            ColourBase('colour',default=None,doc_user="Series colour. Will be selected from colourmap if unspecified.")
                                                                                            ]),allow_missing=False),
                            StringBase('sort',options=('label','label_reverse','value','value_reverse'),default=None,doc_user="How to sort the series. Also applies to the legend, if any.")]
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
                raise Exception, 'xbins==None in BinnedNumericSeriesMixin'
        else:
            xbins = None
        if self.__datamode == 'edge':
            xbins += 1
            
        log_enabled = self.props.get(self.__logsrc,False)
        
        if self.props.sort=='label':
            self.props.series = sorted(self.props.series,key=lambda x:x['label'])
        elif self.props.sort=='label_reverse':
            self.props.series = sorted(self.props.series,key=lambda x:x['label'],reverse=True)
        elif self.props.sort=='value':
            self.props.series = sorted(self.props.series,key=lambda x:sum(x['values']))
        elif self.props.sort=='value_reverse':
            self.props.series = sorted(self.props.series,key=lambda x:sum(x['values']),reverse=True)
        
        for i,series in enumerate(self.props.series):
            if series['colour']==None:
                series['colour']=cmap(float(i)/len(self.props.series))
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
            series['integral'] = sum(series['values'])
            
class LabelledSeriesMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ListElementBase('series',dict,DictElementBase('listitem',False,[
                                                                                            StringBase('label',None,default='',doc_user="Item label."),
                                                                                            FloatBase('value',allow_missing=False,doc_user="Item value."),
                                                                                            ColourBase('colour',default=None,doc_user="Item colour. Set from colourmap if unspecified."),
                                                                                            ElementBase('explode',(int,float),default=0,doc_user="Proportion of radius to explode data.")
                                                                                            ]),allow_missing=False),
                            StringBase('sort',options=('label','label_reverse','value','value_reverse'),default=None,doc_user="How to sort the series. Also applies to the legend, if any.")]
        super(LabelledSeriesMixin,self).__init__(*args,**kwargs)
    def data(self,*args,**kwargs):
        cmap = self.props.colourmap
        for i,series in enumerate(self.props.series):
            series['integral']=series['value']
            if series['colour']==None:
                series['colour']=cmap(float(i)/len(self.props.series))
        if self.props.sort=='label':
            self.props.series = sorted(self.props.series,key=lambda x:x['label'])
        elif self.props.sort=='label_reverse':
            self.props.series = sorted(self.props.series,key=lambda x:x['label'],reverse=True)
        elif self.props.sort=='value':
            self.props.series = sorted(self.props.series,key=lambda x:x['integral'])
        elif self.props.sort=='value_reverse':
            self.props.series = sorted(self.props.series,key=lambda x:x['integral'],reverse=True)
        #super(LabelledSeriesMixin,self).data(*args,**kwargs)

class LabelledSeries2DMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [ListElementBase('series',dict,DictElementBase('listitem',False,[
                                                                                            StringBase('label',None,default='',doc_user="Series label."),
                                                                                            ListElementBase('x',(int,float),allow_missing=False,doc_user="Series x-values."),
                                                                                            ListElementBase('y',(int,float),allow_missing=False,doc_user="Series y-values."),
                                                                                            MarkerBase('marker'),ColourBase('colour',default=None,doc_user="Marker to use for series data points.")
                                                                                            ]),allow_missing=False,doc_user="Series x and y value lists must be the same length."),
                            StringBase('sort',options=('label','label_reverse','value','value_reverse'),default=None,doc_user="How to sort the series. Also applies to the legend, if any.")
                            ]
        super(LabelledSeries2DMixin,self).__init__(*args,**kwargs)
    def data(self,*args,**kwargs):
        cmap = self.props.colourmap
        for i,series in enumerate(self.props.series):
            if series['colour']==None:
                series['colour']=cmap(float(i)/len(self.props.series))
            if not len(series['x'])==len(series['y']):
                if len(series['x'])>len(series['y']):
                    series['y'] += [0.]*(len(series['x'])-len(series['y']))
                else:
                    series['x'] += [0.]*(len(series['y'])-len(series['x']))    
            series['integral'] = sum([x*y for x,y in zip(series['x'],series['y'])])
        if self.props.sort=='label':
            self.props.series = sorted(self.props.series,key=lambda x:x['label'])
        elif self.props.sort=='label_reverse':
            self.props.series = sorted(self.props.series,key=lambda x:x['label'],reverse=True)
        elif self.props.sort=='value':
            self.props.series = sorted(self.props.series,key=lambda x:x['integral'])
        elif self.props.sort=='value_reverse':
            self.props.series = sorted(self.props.series,key=lambda x:x['integral'],reverse=True)
            
        
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
            return "Different row lengths in 'data'"
        if self.__rowlen!=None and not list(lengths)[0]==self.__rowlen:
            return "Rowlength != %s in 'data'"%self.__rowlen
        return True
        #return super(ArrayMixin,self).validate(input)
    def predata(self,*args,**kwargs):
        if self.__min!=None or self.__max!=None:
            for row in self.props.data:
                for i,item in enumerate(row[:]):
                    if self.__min!=None and item<self.__min:
                        row[i]=self.__min
                    if self.__max!=None and item>self.__max:
                        row[i]=self.__max
        
class WatermarkMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.__watermarks = {'cms':'cmslogo.png'}
        self.validators += [StringBase('watermark',options=self.__watermarks.keys(),default=None,doc_user="Name of predefined watermark to use."),
                            FloatBase('watermark_alpha',min=0,max=1,default=0.5,doc_user="How transparent to make the watermark."),
                            StringBase('watermark_location',options=('top','bottom','left','right','topleft','topright','bottomleft','bottomright'),default='topright',doc_user="Where to place the watermark."),
                            FloatBase('watermark_scale',min=0,default=1,doc_user="How much to scale the watermark relative to original size.")]
        super(WatermarkMixin,self).__init__(*args,**kwargs)
        
    def finalise(self,*args,**kwargs):
        if self.props.watermark:
            try:
                image = matplotlib.image.imread(self.__watermarks[self.props.watermark])
                image[:,:,-1] = self.props.watermark_alpha
                width,height = self.props.width,self.props.height
                l,b,w,h = self.props.get('axes_left',0),self.props.get('axes_bottom',0),self.props.get('avail_width',width)/float(width),self.props.get('avail_height',height)/float(height)
                iw = image.shape[0]*self.props.watermark_scale/float(width)
                ih = image.shape[1]*self.props.watermark_scale/float(height)
                
                origin = {
                    'top':[l+w*0.5-iw*0.5,b+h-ih,iw,ih],
                    'left':[l,b+h*0.5-ih*0.5,iw,ih],
                    'bottom':[l+w*0.5-iw*0.5,b,iw,ih],
                    'right':[l+w-iw,b+h*0.5-ih*0.5,iw,ih],
                    'topleft':[l,b+h-ih,iw,ih],
                    'topright':[l+w-iw,b+h-ih,iw,ih],
                    'bottomleft':[l,b,iw,ih],
                    'bottomright':[l+w-iw,b,iw,ih]
                }[self.props.watermark_location]
                
                imaxes = self.figure.add_axes(origin)
                imaxes.set_axis_off()
                imaxes.imshow(image)
            except:
                pass
                
class LegendMixin(Mixin):
    def __init__(self,*args,**kwargs):
        self.validators += [StringBase('legend',options=('top','bottom','left','right','topleft','topright','bottomleft','bottomright'),default=None,doc_user="Location of legend to display."),
                            IntBase('legend_maxitems',min=0,default=-1,doc_user="Maximum items to show in legend, or -1."),
                            ElementBase('legend_value',bool,default=False,doc_user="Whether to append items with their integral."), 
                            ]
        super(LegendMixin,self).__init__(*args,**kwargs)
    def postdata(self):
        axes = self.figure.gca()
        if self.props.legend==None:
            return
        loc = {
                'top':9,
                'left':6,
                'bottom':8,
                'right':7,
                'topleft':2,
                'topright':1,
                'bottomleft':3,
                'bottomright':4
            }.get(self.props.legend,-1)
        if loc!=-1:
            if hasattr(self.props,'legend_items'):
                items = self.props.legend_items
                sort = self.props.get('sort',None)
                if sort=='label':
                    items = sorted(items,key=lambda x: x['label'])
                elif sort=='label_reverse':
                    items = sorted(items,key=lambda x: x['label'],reverse=True)
                elif sort=='value':
                    items = sorted(items,key=lambda x: x['value'])
                elif sort=='value_reverse':
                    items = sorted(items,key=lambda x: x['value'],reverse=True)
                if self.props.legend_maxitems>0:
                    items = items[:self.props.legend_maxitems]
                if self.props.legend_value:
                    formatter = SIFormatter()
                    for item in items:
                        item['label'] = '%s [%s]'%(item['label'],formatter(item['value']))
                axes.legend([item['artist'] for item in items],[item['label'] for item in items],loc=loc)
            else:
                axes.legend(loc=loc)
            