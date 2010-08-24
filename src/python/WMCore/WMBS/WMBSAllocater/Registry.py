#!/usr/bin/env python
"""
_Registry_

Registry for WMBS Allocaters

Note that this is a runtime module, so provides its own registry
rather than using the common implementation to avoid generating
extra runtime dependencies.

"""

from WMCore.WMBS.WMBSAllocater.AllocaterImpl import AllocaterImpl
import Allocaters


class RegistryError:
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
    allocaters = {}

    def __init__(self):
        msg = "Do not init StageOut.Registry class"
        raise RuntimeError, msg


def registerAllocaterImpl(name, classRef):
    """
    _registerStageOutImpl_

    Register a StageOutImpl subclass with the name provided

    """
    if name in Registry.allocaters.keys():
        msg = "Duplicate WMBSAllocater registered for name: %s\n" % name
        raise RegistryError, msg

    if not issubclass(classRef, AllocaterImpl):
        msg = "WMBSAllocater object registered as %s\n" % name
        msg += "is not a subclass of WMBSAllocater.AllocaterImpl\n"
        msg += "Registration should be of a class that inherits AllocaterImpl"
        raise RegistryError, msg

    Registry.allocaters[name] = classRef
    return


def retrieveAllocaterImpl(name, wmbs):
    """
    _retrieveStageOutImpl_

    Get the matching impl class and return an instance of it

    """
    classRef = Registry.allocaters.get(name, None)
    if classRef == None:
        msg = "Failed to find WMBSAllocater for name: %s\n" % name
        raise RegistryError, msg
    return classRef(wmbs)