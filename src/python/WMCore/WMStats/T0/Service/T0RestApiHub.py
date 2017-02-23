"""
Module defines REST API methods and their handles.
Implementation of handles is in corresponding modules, not here.

"""
from __future__ import (division, print_function)
import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi
from WMCore.REST.Format import JSONFormat

from WMCore.WMStats.Service.MetaDataInfo import ServerInfo
from WMCore.WMStats.Service.ActiveRequestJobInfo import ActiveRequestJobInfo
from WMCore.WMStats.Service.RequestInfo import JobDetailInfo

class T0RestApiHub(RESTApi):
    """
    Server object for REST data access API.

    """
    def __init__(self, app, config, mount):
        """
        :arg app: reference to application object; passed to all entities.
        :arg config: reference to configuration; passed to all entities.
        :arg str mount: API URL mount point; passed to all entities."""

        RESTApi.__init__(self, app, config, mount)

        cherrypy.log("T0WMStats entire configuration:\n%s" % Configuration.getInstance())
        cherrypy.log("T0WMStats REST hub configuration subset:\n%s" % config)
        # only allows json format for return value
        self.formats =  [('application/json', JSONFormat())]
        self._add({"info": ServerInfo(app, self, config, mount),
                   "requestcache": ActiveRequestJobInfo(app, self, config, mount),
                   "jobdetail": JobDetailInfo(app, self, config, mount, t0flag=True),
                  })
