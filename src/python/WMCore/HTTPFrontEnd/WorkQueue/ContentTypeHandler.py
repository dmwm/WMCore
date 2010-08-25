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
        """
        TODO: corrently it only works with cjson not json from python2.6.
        There is issues of converting unit code to string.
        """
        from WMCore.Wrappers import JsonWrapper
        params = request.body.read()
        if params:
            kw = JsonWrapper.loads(params)
            kwargs.update(kw)
        return args, kwargs
    
    def jsonThunkerHandler(self, args, kwargs):
        
        args, kwargs = self.jsonHandler(args, kwargs)
        kwargs = JSONThunker.unthunk(kwargs)
        return args, kwargs