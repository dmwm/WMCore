#!/usr/bin/env python
from WMCore.WebTools.Page import DatabasePage, exposexml, exposejson, exposedasjson 
from WMCore.Lexicon import sitetier, countrycode
from cherrypy import expose
import sys

class WebAPI(DatabasePage):
    def __init__(self, config = {}, database = ''):
        DatabasePage.__init__(self, config, database) 
        self.methods = {}
    
    @expose
    def index(self):
        return self.templatepage('API', methods = self.methods, 
                                 application = self.config.application)
    
    @expose
    def default(self, *args, **kwargs):
        return self.index()
    
    @exposejson
    def json(self, *args, **kwargs):
        dict = self.runMethod(args[0], kwargs)
        return dict
    
    @exposedasjson
    def das(self, *args, **kwargs):
        dict = self.runMethod(args[0], kwargs)
        return dict
    
    @exposexml
    def xml(self, *args, **kwargs):
        dict = self.runMethod(args[0], kwargs)
        return dict
    
    def runMethod(self, method, input):
        """
        Run the specified method with the provided input, return a dict 
        containing the result of the call or an exception. 
        """
        kwargs=''
        for i in input:
            kwargs = kwargs + "%s='%s'," % (i, input[i])
        str = "self.methods[method]['call']"
        dict = {}
        try:
            method = eval(str)
            dict = method(kwargs.strip(','))
        except Exception, e:
            error = e.__str__()
            self.debug(error)
            self.debug(str)
            self.debug("%s:%s" % (sys.exc_type, sys.exc_value))
            dict = {'Exception':{'Exception_thrown_in': method.__str__(),
                       'Exception_type': '%s' % sys.exc_type,
                       'Exception_detail':error, 
                       'Exception_string': str, 
                       'Exception_arguments': kwargs,
                       'Exception_dict':dict}}
        return dict