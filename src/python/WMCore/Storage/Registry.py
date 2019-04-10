#!/usr/bin/env python
"""
_Registry_

Registry for StageOutImpl derived plugins

Note that this is a runtime module, so provides its own registry
rather than using the common implementation to avoid generating
extra runtime dependencies.

"""
import WMCore.WMFactory
from WMCore.Storage.StageOutError import StageOutError
from WMCore.Storage.StageOutImpl import StageOutImpl


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
        raise RuntimeError(msg)


def registerStageOutImpl(name, classRef):
    """
    _registerStageOutImpl_

    Register a StageOutImpl subclass with the name provided

    """
    if name in Registry.StageOutImpl.keys():
        msg = "Duplicate StageOutImpl registered for name: %s\n" % name
        raise RegistryError(msg)

    if not issubclass(classRef, StageOutImpl):
        msg = "StageOutImpl object registered as %s\n" % name
        msg += "is not a subclass of StageOut.StageOutImpl\n"
        msg += "Registration should be of a class that inherits StageOutImpl"
        raise RegistryError(msg)

    Registry.StageOutImpl[name] = classRef
    return


def retrieveStageOutImpl(name, stagein=False, useNewVersion=False):
    """
    _retrieveStageOutImpl_

    Get the matching impl class and return an instance of it

    """
    if not useNewVersion:
        classRef = Registry.StageOutImpl.get(name, None)
    else:
        try:
            return _retrieveStageOutImpl2(name)
        except ImportError:
            raise RegistryError("Stageout plugin %s doesn't exist" % name)

    if not useNewVersion:
        return classRef(stagein)
    else:
        return classRef()


pluginLookup = {'test-win': 'TestWinImpl',
                'test-fail': 'TestFailImpl',
                'test-copy': 'TestLocalCopyImpl',
                "cp": "CPImpl",
                "stageout-xrdcp-fnal": 'FNALImpl',
                "srmv2": 'SRMV2Impl',
                "srmv2-lcg": 'LCGImpl',
                "xrdcp": 'XRDCPImpl',
                "vandy": 'VandyImpl',
                "gfal2": 'GFAL2Impl',
                "awss3": 'AWS3Impl',
                # NOTE NOTE NOTE:
                # do NOT implement this
                "testFallbackToOldBackend": 'TestBackendForFallbacksDontImplement'}


def _retrieveStageOutImpl2(backendName):
    factory = WMCore.WMFactory.WMFactory(name='StageOutFactory',
                                         namespace='WMCore.Storage.Plugins')
    className = pluginLookup[backendName]
    stageout = factory.loadObject(className)
    return stageout
