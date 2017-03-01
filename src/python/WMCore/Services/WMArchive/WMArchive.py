from __future__ import (division, print_function)

import json

from WMCore.Services.Service import Service


class WMArchive(Service):
    """
    This is skelton class which need be implemented.
    """
    def __init__(self, url, header = {}):
        """
        responseType will be either xml or json
        """

        httpDict = {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        httpDict.setdefault("accept_type", "application/json")
        httpDict.update(header)
        self.encoder = json.dumps
        Service.__init__(self, httpDict)



    def archiveData(self, data):
        return self["requests"].post('', {'data': data})[0]['result']
