#!/usr/bin/env python
"""
_Registry_

Registry for WMBS feeders

Note that this is a runtime module, so provides its own registry
rather than using the common implementation to avoid generating
extra runtime dependencies.

"""

from WMCore.WMBS.WMBSFeeder.FeederImpl import FeederImpl
import Feeders


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
    feeders = {}

    def __init__(self):
        msg = "Do not init StageOut.Registry class"
        raise RuntimeError, msg


def registerFeederImpl(name, classRef):
    """
    _registerStageOutImpl_

    Register a StageOutImpl subclass with the name provided

    """
    if name in Registry.feeders.keys():
        msg = "Duplicate WMBSFeeder registered for name: %s\n" % name
        raise RegistryError, msg

    if not issubclass(classRef, FeederImpl):
        msg = "WMBSFeeder object registered as %s\n" % name
        msg += "is not a subclass of WMBSFeeder.FeederImpl\n"
        msg += "Registration should be of a class that inherits FeederImpl"
        raise RegistryError, msg

    Registry.feeders[name] = classRef
    return

def retrieveFeederImpl(name, wmbs):
    """
    _retrieveStageOutImpl_

    Get the matching impl class and return an instance of it

    """
    classRef = Registry.feeders.get(name, None)
    if classRef == None:
        msg = "Failed to find WMBSFeeder for name: %s\n" % name
        raise RegistryError, msg
    return classRef(wmbs)