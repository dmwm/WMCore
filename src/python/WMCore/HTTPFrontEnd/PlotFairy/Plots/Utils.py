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

def elem(what,inner='',**kwargs):
    argstr = ' '.join([what]+["%s='%s'"%(k,v) for k,v in kwargs.items()])
    return '<%s>%s</%s>'%(argstr,inner,what)

def text_size(string,fontsize,dpi=100):
    return 0.6*len(string)*(dpi/72.)*fontsize,1.4*(dpi/72.)*fontsize

def font_size(string,space,dpi=100):
    return space/(0.6*(len(string)+2)*(dpi/72.))

        
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
    formats = {
               'second':'%M:%S',
               'minute':'%H:%M',
               'hour':'%a %H:%M',
               'day':'%m %d',
               'week':'%b %d',
               'month':'%d %b %Y',
               'year':'%b %Y',
               'decade':'%Y'
               }
    def __init__(self,span='hour'):
        if span in TimeFormatter.formats:
            self.time_format = TimeFormatter.formats[span]
        else:
            self.time_format = span
        matplotlib.ticker.ScalarFormatter.__init__(self,useOffset=False)
    def __call__(self,val,pos=None):
        return time.strftime(self.time_format,time.localtime(val))
                
class HexFormatter(matplotlib.ticker.ScalarFormatter):
    def __init__(self):
        matplotlib.ticker.ScalarFormatter.__init__(self,useOffset=False)
    def __call__(self,val,pos=None):
        return '%x'%val           

class SuffixFormatter(matplotlib.ticker.ScalarFormatter):
    suffix = [(1,'')]
    def __init__(self):
        matplotlib.ticker.ScalarFormatter.__init__(self,useOffset=False)
    def __call__(self,val,pos=None):
        for s in self.__class__.suffix:
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