#!/usr/bin/env python
"""
_Registry_

Registry for StageOutImpl derived plugins

Note that this is a runtime module, so provides its own registry
rather than using the common implementation to avoid generating
extra runtime dependencies.

"""

from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

class RegistryError(StageOutError):
    """
    _RegistryError_

    Error class for handling registry issues
    """
    pass

class Registry:
    """
    _Registry_

    Singleton namespace for storing static reference to all
    stage out impl class objects

    """
    StageOutImpl = {}

    def __init__(self):
        msg = "Do not init StageOut.Registry class"
        raise RuntimeError, msg



    

def registerStageOutImpl(name, classRef):
    """
    _registerStageOutImpl_

    Register a StageOutImpl subclass with the name provided

    """
    if name in Registry.StageOutImpl.keys():
        msg = "Duplicate StageOutImpl registered for name: %s\n" % name
        raise RegistryError, msg

    
    if not issubclass(classRef, StageOutImpl):
        msg = "StageOutImpl object registered as %s\n" % name
        msg += "is not a subclass of StageOut.StageOutImpl\n"
        msg += "Registration should be of a class that inherits StageOutImpl"
        raise RegistryError, msg

    Registry.StageOutImpl[name] = classRef
    return

def retrieveStageOutImpl(name, stagein=False):
    """
    _retrieveStageOutImpl_

    Get the matching impl class and return an instance of it

    """
    classRef = Registry.StageOutImpl.get(name, None)
    if classRef == None:
        msg = "Failed to find StageOutImpl for name: %s\n" % name
        raise RegistryError, msg
    return classRef(stagein)

        
