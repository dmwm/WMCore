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

# MSPileup get APIs
# spec.json will contains spec query to delete MSPileup document, e.g.
# {"query": {"pileupName": "bla-bla-bla"}}
curl -X POST -H "Content-type: application/json" -d@./spec.json http://lhost:port/ms-pileup/data

or we will use dedicated end-point, e.g. data
curl -X GET http://lhost:port/ms-pileup/data/pileup?pileupName=bla
curl -X GET http://lhost:port/ms-pileup/data/pileup?campaign=campaign

# MSPileup create APIs
# doc.json contains new MSPileup document
curl -X POST -H "Content-type: application/json" -d@./doc.json http://lhost:port/ms-pileup/data

# MSPileup update API
curl -X PUT -H "Content-type: application/json" -d '{"pileupName":"bla"}' http://localhost:8822/microservice/data

# MSPileup delete API
curl -X DELETE -H "Content-type: application/json" -d '{"pileupName":"bla"}' http://localhost:8822/microservice/data
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


def mspileupError(doc):
    """
    Check MSPileup record for error
    :return: None or raise cherrypy.HTTPError
    """
    if 'error' in doc:
        msg = doc['message']
        code = doc['code']
        msg = f'MSPileupError: {msg}, code: {code}'
        raise cherrypy.HTTPError(400, msg) from None


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
        self.mount = mount
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

        api = kwds.get('API')

        if api == "status":
            detail = True if kwds.pop("detail", True) in (True, "true", "True", "TRUE") else False
            res.update(self.mgr.status(detail, **kwds))
            yield res
        elif api == "info":
            res.update(self.mgr.info(kwds.pop("request", None), **kwds))
            yield res
        elif 'ms-pileup' in self.mount:
            # this section of the GET API is only relevant for MSPileup service
            # the self.mount point should match service configuration, e.g. in MSPileup we define
            # mount point as /ms-pileup/data, e.g. http://.../ms-pileup/data
            try:
                for doc in self.mgr.get(**kwds):
                    mspileupError(doc)
                    yield doc
            except cherrypy.HTTPError:
                raise
            except:
                msg = 'Unable to GET request, error=%s' % traceback.format_exc()
                raise cherrypy.HTTPError(status=500, message=msg) from None

    @restcall(formats=[('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self):
        """
        Implement POST request API.
        The input HTTP request should be in the following form
        {"request":some_data} for posting the data into the service.

        NOTE: the usage of this method requires further thought
        """
        try:
            data = json.load(cherrypy.request.body)
            if 'ms-pileup' in self.mount:
                # this section of POST API is only relevant for MSPileup service
                for doc in self.mgr.post(data):
                    mspileupError(doc)
                    yield doc
            elif 'ms-transferor' in self.mount:
                for doc in self.mgr.post(data):
                    yield doc
            else:
                msg = f"End point {self.mount} does not support POST request JSON payload"
                raise cherrypy.HTTPError(status=400, message=msg) from None
        except cherrypy.HTTPError:
            raise
        except:
            msg = 'Unable to POST request, error=%s' % traceback.format_exc()
            raise cherrypy.HTTPError(status=500, message=msg) from None

    @restcall(formats=[('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def put(self):
        """
        Implement PUT HTTP method of MicroService API.
        """
        try:
            if 'ms-pileup' in self.mount:
                # this section of PUT API is only relevant for MSPileup service
                data = json.load(cherrypy.request.body)
                for doc in self.mgr.update(data):
                    mspileupError(doc)
                    yield doc
        except cherrypy.HTTPError:
            raise
        except:
            msg = 'Unable to PUT request, error=%s' % traceback.format_exc()
            raise cherrypy.HTTPError(status=500, message=msg) from None

    @restcall(formats=[('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def delete(self):
        """
        Implement DELETE HTTP method of MicroService API.
        """
        if 'ms-pileup' not in self.mount:
            return # not implemented for other MS services

        # DELETE API is only relevant for MSPileup service
        spec = {}
        try:
            spec = json.load(cherrypy.request.body)
        except:
            msg = 'Unable to DELETE request, error=%s' % traceback.format_exc()
            raise cherrypy.HTTPError(status=400, message=msg) from None
        # so far allow only pileupName in input spec
        if 'pileupName' not in spec:
            msg = "wrong spec, please provide pileupName"
            raise cherrypy.HTTPError(status=400, message=msg) from None
        try:
            if 'pileupName' in spec and len(spec.keys()) == 1:
                for doc in self.mgr.delete(spec):
                    mspileupError(doc)
                    yield doc
        except cherrypy.HTTPError:
            raise
        except:
            msg = 'Unable to DELETE request, error=%s' % traceback.format_exc()
            raise cherrypy.HTTPError(status=500, message=msg) from None
