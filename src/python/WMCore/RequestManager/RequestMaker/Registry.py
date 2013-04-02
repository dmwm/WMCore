#!/usr/bin/env python
"""
_Registry_

Registry and API methods to track pairs of RequestMaker and RequestSchema
classes and instantiate them via a factory method

Note that a schema is retrieved via its corresponding maker.

"""
import copy
import time
from WMCore.RequestManager.DataStructs.Request import Request

class _Registry:
    """
    Internal namespace to keep maps of request type to instances

    """

    _Makers = {}
    _Schemas = {}
    _Factories = {}

    def __init__(self):
        msg = "Do not init ths class"
        raise RuntimeError, msg



def registerRequestType(typename, makerClass, schemaClass):
    """
    _registerRequestType_

    Register a request type with its maker class and schema class

    """
    _Registry._Makers[typename] = makerClass
    _Registry._Schemas[typename] = schemaClass

    return



def retrieveRequestMaker(typename):
    """
    _retrieveRequestMaker_

    Retrieve a request maker instance for the named type.

    """
    if not _Registry._Makers.has_key(typename):
        print _Registry._Makers.keys()
        msg = "No RequestMaker implementation registered with name:"
        msg += " %s" % typename
        raise RuntimeError, msg

    maker = _Registry._Makers[typename]()
    maker.requestType = typename
    maker.schemaClass = _Registry._Schemas[typename]

    return maker


def buildWorkloadForRequest(typename, schema):
    """
    _buildWorkloadForRequest_

    Prototype master class for ReqMgr request creation

    Should load factory, use the schema to find arguments,
    validate the arguments, and then return a finished workload.
    """
    requestName = schema['RequestName']

    if not typename in _Registry._Factories.keys():
        factoryName = '%sWorkloadFactory' % typename
        try:
            mod = __import__('WMCore.WMSpec.StdSpecs.%s' % typename,
                             globals(), locals(), [factoryName])
            factoryInstance = getattr(mod, factoryName)
        except ImportError:
            msg =  "Spec type %s not found in WMCore.WMSpec.StdSpecs" % typename
            raise RuntimeError, msg
        except AttributeError, ex:
            msg = "Factory not found in Spec for type %s" % typename
            raise RuntimeError, msg
        _Registry._Factories[typename] = factoryInstance
    else:
        factoryInstance = _Registry._Factories[typename]

    # So now we have a factory
    # Time to run it
    # Any exception here will be caught at a higher level (ReqMgrWebTools)
    # This should also handle validation
    factory  = factoryInstance()
    workload = factory.factoryWorkloadConstruction(workloadName = requestName,
                                                   arguments = schema)

    # Now build a request
    request = Request()
    request.update(schema)
    loadRequestSchema(workload = workload, requestSchema = schema)
    request['WorkloadSpec'] = workload.data
    # use the CMSSWVersion from the input schema only if it's defined (like
    # for a new request). for example for a resubmission request, schema['CMSSWVersion']
    # is empty and will be worked out later ; do not use any defaults
    # TODO:
    # all these fiddling along the route of creating the request should be concentrated
    #    at single place! Otherwise implementation of things like request cloning is
    #    unnecessary complicated for there is a lot hidden manipulations
    #    for for cloning - do this only if it's not defined already!
    #    seeing what is written above about resubmission, not sure if for resubmission
    #    this is not now screwed up
    if schema.get('CMSSWVersion') and schema.get('CMSSWVersion') not in request['SoftwareVersions']:
        request['SoftwareVersions'].append(schema.get('CMSSWVersion'))

    # Usually DbsUrl is not in the schema, and gets a default value in the WMSpec creation
    # Use the top level task DBSUrl as the DBSUrl for the whole request, store it in CouchDB
    request['DbsUrl'] = (workload.getTopLevelTask()[0]).dbsUrl()

    return request


def loadRequestSchema(workload, requestSchema):
    """
    _loadRequestSchema_

    Does modifications to the workload I don't understand
    Takes a WMWorkloadHelper, operates on it directly with the schema
    """
    schema = workload.data.request.section_('schema')
    for key, value in requestSchema.iteritems():
        try:
            setattr(schema, key, value)
        except Exception, ex:
            # Attach TaskChain tasks
            if type(value) == dict and requestSchema['RequestType'] == 'TaskChain' and 'Task' in key:
                newSec = schema.section_(key)
                for k, v in requestSchema[key].iteritems():
                    try:
                        setattr(newSec, k, v)
                    except Exception, ex:
                        pass
            else:
                pass
    schema.timeStamp = int(time.time())
    schema = workload.data.request.schema

    # might belong in another method to apply existing schema
    workload.data.owner.Group = schema.Group
    workload.data.owner.Requestor = schema.Requestor
    if hasattr(schema, 'RequestPriority'):
        workload.data.request.priority = schema.RequestPriority
