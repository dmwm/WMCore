# pylint: disable=E0239
# E0239: inherit-non-class
"""
File       : Data.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: This module consists of all REST APIs required by MicroService

Client may invoke GET/POST calls to MicroService, e.g.
# return status of the MicroService
curl http://localhost:8822/microservice/data/status
# ping MicroService, i.e. return its default message
curl http://localhost:8822/microservice/data
# post data to MicroService
curl -X POST -H "Content-type: application/json" -d '{"request":{"spec":"spec"}}' http://localhost:8822/microservice/data
"""
# futures
from __future__ import print_function, division

# system modules
import json
import traceback
import importlib
from future.utils import with_metaclass
# from types import GeneratorType

# 3d party modules
import cherrypy

# WMCore modules
import WMCore
from Utils.Patterns import Singleton
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
# from WMCore.REST.Validation import validate_rx, validate_str
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat


def results(res):
    "Return results in a list format suitable by REST server"
    if not isinstance(res, list):
        return [res]
    return res

class Data(with_metaclass(Singleton, RESTEntity, object)):
    """
    This class is responsbiel for both the REST interface
    and the application/service itself, thus make it a
    Singleton to guarantee that only one instance will be
    executing
    """
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.config = config
        arr = config.manager.split('.')
        try:
            cname = arr[-1]
            module = importlib.import_module('.'.join(arr[:-1]))
            self.mgr = getattr(module, cname)(config)
        except ImportError:
            print("ERROR initializing MicroService REST module.")
            traceback.print_exc()

    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.

        """
        if method == 'GET':
            for prop in list(param.kwargs):
                safe.kwargs[prop] = param.kwargs.pop(prop)
            safe.kwargs['API'] = api
            if param.args:
                return False
        elif method == 'POST':
            if not param.args or not param.kwargs:
                return False
        return True

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, **kwds):
        """
        Implement GET request with given uid or set of parameters
        """
        res = {'wmcore_version': WMCore.__version__,
               'microservice_version': WMCore.__version__,  # FIXME: extract it from another place
               'microservice': self.mgr.__class__.__name__}

        if kwds.get('API') == "status":
            detail = True if kwds.pop("detail", True) in (True, "true", "True", "TRUE") else False
            res.update(self.mgr.status(detail, **kwds))
        elif kwds.get('API') == "info":
            res.update(self.mgr.info(kwds.pop("request", None), **kwds))
        return results(res)

    @restcall(formats=[('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self):
        """
        Implement POST request API.
        The input HTTP request should be in the following form
        {"request":some_data} for posting the data into the service.

        NOTE: the usage of this method requires further thought
        """
        msg = 'expect "request" attribute in your request'
        result = {'status': 'Not supported, %s' % msg, 'request': None}
        try:
            data = json.load(cherrypy.request.body)
            if 'request' in data:
                reqName = data['request']
                result = self.mgr.info(reqName)
            return results(result)
        except cherrypy.HTTPError:
            raise
        except:
            msg = 'Unable to POST request, error=%s' % traceback.format_exc()
            raise cherrypy.HTTPError(status=500, message=msg)
