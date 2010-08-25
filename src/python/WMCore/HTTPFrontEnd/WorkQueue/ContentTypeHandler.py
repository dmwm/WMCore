from cherrypy import request
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

class ContentTypeHandler:
    
    def __init__(self):
        self.supportedType = {"application/json": self.jsonHandler,
                              "text/json": self.jsonHandler,
                              "application/json+thunker": self.jsonThunkerHandler,
                              "text/json+thunker": self.jsonThunkerHandler,}
        
    def convertToParam(self, args, kwargs):
        func = self.supportedType.get(request.headers["Content-Type"])
        if func == None:
            return args, kwargs
        else:
            return func(args, kwargs)
    
    def jsonHandler(self, args, kwargs):
        
        from WMCore.Wrappers import JsonWrapper
        print "&&&&&&&&&& ------"
        params = request.body.read()
        print params
        kw = JsonWrapper.loads(params)
        kwargs.update(kw)
        return args, kwargs
    
    def jsonThunkerHandler(self, args, kwargs):
        
        args, kwargs = self.jsonHandler(args, kwargs)
        kwargs = JSONThunker.unthunk(kwargs)
        return args, kwargs