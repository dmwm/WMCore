import matplotlib
import matplotlib.colors
import matplotlib.image
import matplotlib.ticker
import re

#Some of this code is pinched from Overview/DQMGui plotting

class ElementBase:
    name = None
    def validate(input):
        "Validate whether a critical error occurred and the plot cannot be made. Defaults should be applied at the point of getting data"
        return True#False

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

class TitleElement(BaseElement):
    def validate(input):
        
class CanvasElement(BaseElement):
    def validate(input):