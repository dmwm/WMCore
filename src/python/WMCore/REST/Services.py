from __future__ import print_function, division

import time
from Utils.ProcessStats import processStatusDict
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.REST.Server import RESTEntity, restcall, rows

class ProcessMatrix(RESTEntity):
    """
    Process and thread matrix in reqmgr2
    """
    def __init__(self, app, api, config, mount):
        # CouchDB auxiliary database name
        RESTEntity.__init__(self, app, api, config, mount)
        self.time0 = time.time()

    def validate(self, apiobj, method, api, param, safe):
        pass

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    def get(self):
        sdict = {'server': processStatusDict()}
        sdict['server'].update({'uptime': time.time() - self.time0})
        return rows([sdict])