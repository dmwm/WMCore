from cherrypy import request
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

class ContentTypeHandler:
    
    def __init__(self):
        self.supportedType = {"application/json": self.jsonHandler,
                              "text/json": self.jsonHandler,
                              "application/json+thunker": self.jsonThunkerHandler,
                              "text/json+thunker": self.jsonThunkerHandler,}
        
    def convertToParam(self, args, kwargs):
        func = self.supportedType.get(request.headers.setdefault("Content-Type", "text/json"))
        if func == None:
            return args, kwargs
        else:
            return func(args, kwargs)
    
    def jsonHandler(self, args, kwargs):
        """
        TODO: Ugly temporary hack to convert unicode to string for kwargs
              Only tested on python26 built-in json
        """
        #if get verb doesn't have request.boby 
        #TODO: maybe this should filtered on upper level
        if request.body != None:
            from WMCore.Wrappers import JsonWrapper
            params = request.body.read()
            if params:
                kw = JsonWrapper.loads(params)
                kwargs.update(kw)
        return args, kwargs
    
    def jsonThunkerHandler(self, args, kwargs):
        
        args, kwargs = self.jsonHandler(args, kwargs)
        kwargs = JSONThunker().unthunk(kwargs)
        return args, kwargs