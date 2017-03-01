"""
Meta data information API.

"""

import WMCore

from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Format import JSONFormat


class ServerInfo(RESTEntity):
    def validate(self, apiobj, method, api, param, safe):
        """

        """
        pass

    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self):
        """
        """
        wmstats_version = WMCore.__version__
        return "version: %s" % wmstats_version
