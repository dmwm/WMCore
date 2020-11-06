#!/usr/bin/env python
"""
_ScriptFactory_

WMFactory for instantiating runtime implementations of the ScriptInterface

"""

from builtins import str
from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException

class ScriptFactoryException(WMException):
    """
    _ScriptFactortyException_

    Exception for missing objects or problems

    """
    pass



class ScriptFactory(WMFactory):
    """
    _ScriptFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMRuntime.Scripts")

_ScriptFactory = ScriptFactory()

def getScript(scriptName):
    """
    _getScript_

    factory method to return a ScriptInterface impl instance based on the
    name provided

    """
    try:
        return _ScriptFactory.loadObject(scriptName)
    except WMException as wmEx:
        msg = "ScriptFactory Unable to load Object: %s" % scriptName
        raise ScriptFactoryException(msg)
    except Exception as ex:
        msg = "Error creating object %s in ScriptFactory:\n" % scriptName
        msg += str(ex)
        raise ScriptFactoryException(msg)
