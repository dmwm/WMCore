from cherrypy import request


class ContentTypeHandler:
    
    def __init__(self):
        self.supportedType = {"application/json": self.jsonHandler,
                              "text/json": self.jsonHandler}
        
    def convertToParam(self, kwargs):
        func = self.supportedType.get(request.headers["Content-Type"])
        if func == None:
            return kwargs
        else:
            return func()
    
    def jsonHandler(self):
        from WMCore.Wrappers import jsonwrapper
        print "&&&&&&&&&& ------"
        #print request.body.read()
        return jsonwrapper.loads(request.body.read(), encoding="latin-1")