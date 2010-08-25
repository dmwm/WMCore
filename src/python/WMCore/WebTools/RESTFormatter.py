from WMCore.WebTools.Page import TemplatedPage, exposejson, exposexml, exposeatom

class RESTFormatter(TemplatedPage):
    @exposejson
    def json(self, data):
        return data
    
    @exposexml
    def xml(self, data):
        return data

    @exposeatom
    def atom(self, data):
        return data