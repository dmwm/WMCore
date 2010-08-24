#!/usr/bin/env python
"""
_Registry_

Registry and API methods to track pairs of RequestMaker and RequestSchema
classes and instantiate them via a factory method

Note that a schema is retrieved via its corresponding maker.

"""




class _Registry:
    """
    Internal namespace to keep maps of request type to instances

    """

    _Makers = {}
    _Schemas = {}
    
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


