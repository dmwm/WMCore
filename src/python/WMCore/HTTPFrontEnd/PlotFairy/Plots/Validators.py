import copy
import matplotlib.colors
import re
from Utils import elem

class ElementBase:
    def __init__(self,element_name,element_type=None,allow_missing=True,default=None,doc_user=''):
        self.element_name = element_name
        self.element_type = element_type
        self.allow_missing = allow_missing
        self.default = default
        self.doc_user=doc_user
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
    def doc(self):
        header = elem('b',self.element_name)
        if not hasattr(self.element_type,'__name__'):
            header += ' (%s) '%', '.join([el.__name__ for el in self.element_type])
        else:
            header += ' (%s) '%self.element_type.__name__
        if self.allow_missing:
            header += elem('i','Optional')
        else:
            header += elem('b','Required')
        header += ' default=%s' % self.default
        return elem('li',elem('div',elem('div',header)+elem('div',self.doc_extra())+elem('div',elem('i',self.doc_user))))
    def doc_extra(self):
        return ''

class MarkerBase(ElementBase):
    def __init__(self,element_name,default='o',doc_user=''):
        ElementBase.__init__(self,element_name,basestring,True,default,doc_user)
    def extract(self,input):
        if self.element_name in input:
            if input[self.element_name] in 'so^>v<dph8+x,.*':
                return input[self.element_name]
            else:
                return self.default
        else:
            return self.default
    def doc_extra(self):
        return "Must be a valid matplotlib marker definition (eg so^>v<dph8+x,.*)" 

class ColourBase(ElementBase):
    def __init__(self,element_name,default='black',doc_user=''): #any colour you want, providing it's...
        ElementBase.__init__(self,element_name,basestring,True,default,doc_user)
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
    def doc_extra(self):
        return "Must be a valid matplotlib colour; either a CSS colour name or an RGB specification #xxxxxx"
       
class IntBase(ElementBase):
    def __init__(self,element_name,min=None,max=None,allow_missing=True,default=None,doc_user=''):
        ElementBase.__init__(self,element_name,(int,float),allow_missing,default,doc_user)
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
    def doc_extra(self):
        result = ['Integer']
        if self.min!=None:
            result += ['value >= %s'%self.min]
        if self.max!=None:
            result += ['value <= %s'%self.max]
        return ' '.join(result)
        
class FloatBase(ElementBase):
    def __init__(self,element_name,min=None,max=None,allow_missing=True,default=None,doc_user=''):
        ElementBase.__init__(self,element_name,(int,float),allow_missing,default,doc_user)
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
    def doc_extra(self):
        result = ['Floating-point']
        if self.min!=None:
            result += ['value >= %s'%self.min]
        if self.max!=None:
            result += ['value <= %s'%self.max]
        return ' '.join(result)
            
class StringBase(ElementBase):
    def __init__(self,element_name,options=None,default=None,doc_user=''):
        ElementBase.__init__(self,element_name,basestring,True,default,doc_user)
        self.options = options
    def extract(self,input):
        if self.element_name in input:
            val = input[self.element_name]
            if (not self.options==None) and (not val in self.options):
                val = self.default
            return val
        else:
            return self.default
    def doc_extra(self):
        if self.options:
            return 'Valid options: %s'%', '.join(self.options)
        else:
            return ''
        
class FontFamily(StringBase):
    def __init__(self,element_name,default='serif',doc_user=''):
        StringBase.__init__(self,element_name,('serif','sans-serif','monospace'),default,doc_user)
        
class FontWeight(StringBase):
    def __init__(self,element_name,default='normal',doc_user=''):
        StringBase.__init__(self,element_name,('light','normal','bold'),default,doc_user)

class ColourMap(StringBase):
    def __init__(self,element_name,default='Accent',doc_user=''):
        ElementBase.__init__(self,element_name,basestring,True,default,doc_user)
    def extract(self,input):
        if self.element_name in input:
            val = input[self.element_name]
            if matplotlib.cm.get_cmap(val):
                return matplotlib.cm.get_cmap(val)
        return matplotlib.cm.get_cmap(self.default)
    def doc_extra(self):
        return 'Must be a matplotlib colourmap name'
               
class FontSize(ElementBase):
    def __init__(self,element_name,default=None,doc_user=''):
        ElementBase.__init__(self,element_name,(basestring,int),True,default,doc_user)
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
    def doc_extra(self):
        return 'Must be a number or a size description [[x]x-]small|medium|large'        
        
class ListElementBase(ElementBase):
    def __init__(self,element_name,list_element_type=None,item_validator=None,min_elements=None,max_elements=None,allow_missing=True,default=None,doc_user=''):
        ElementBase.__init__(self,element_name,(list,tuple),allow_missing,copy.deepcopy(default),doc_user)
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
    def doc_extra(self):
        result = []
        if self.min_elements!=None:
            result += ['Number of elements >= %s'%self.min_elements]
        if self.max_elements!=None:
            result += ['Number of elements <= %s'%self.max_elements]
        if self.item_validator!=None:
            result += [elem('ul',self.item_validator.doc())]
        return ''.join([elem('div',r) for r in result])
        
class DictElementBase(ElementBase):
    def __init__(self,element_name,allow_missing=True,validate_elements=None,doc_user=''):
        ElementBase.__init__(self,element_name,dict,allow_missing,None,doc_user)
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
    def doc_extra(self):
        if self.validate_elements!=None:
            return elem('ul',''.join([ve.doc() for ve in self.validate_elements]))
        else:
            return ''