#!/usr/bin/env python
"""
_Config_

Pseudo CMSSW config classes.
"""

class Container():
    """
    _Container_

    Empty class to emulate a PSet.
    """
    _type = None

    def setType(self, type):
        """
        _setType_

        Set the type of the object.
        """
        self._type = type
        return

    def type_(self):
        """
        _type__

        Retrieve the type of the object.
        """
        return self._type

class ConfigObject():
    """
    _ConfigObject_

    Emulate some sort of configuration parameter.
    """
    def __init__(self, type, value):
        self.type = type
        self.value = value
        return

    def setValue(self, value):
        self.value = value

    def __getitem__(self, item):
        if isinstance(self.value, list):
            return self.value[item]

class _Untracked(object):
    """
    _Untracked_

    Wrapper for untracker parameters.
    """
    @staticmethod
    def __call__(param):
        return param

    def __getattr__(self, name):
        class Factory(object):
            def __init__(self, name):
                self.name = name

            def __call__(self, *value, **params):
                param = globals()[self.name](*value, **params)
                return _Untracked.__call__(param)

        return Factory(name)

untracked = _Untracked()

def string(stringContents = None):
    """
    _string_

    String configuration parameter.
    """
    return ConfigObject("string", stringContents)

def vstring(stringContents = None):
    """
    _vstring_

    VString configuration parameter.
    """
    return ConfigObject("vstring", stringContents)

def uint32(value = 0):
    """
    _uint32_

    Unsigned 32bit integer parameter.
    """
    return ConfigObject("uint32", value)

def int32(value = 0):
    """
    _int32_

    Signed integer parameter.
    """
    return ConfigObject("uint32", value)

def bool(value = True):
    """
    _bool_

    Bool parameter.
    """
    return ConfigObject("bool", value)

def VLuminosityBlockRange():
    """
    _VLuminosityBlockRange_

    Array of luminosity block ranges.
    """
    return []

def PSet(**attributes):
    """
    _PSet_

    Create a new PSet with the given attributes.
    """
    newPSet = Container()
    for attributeName in attributes.keys():
        setattr(newPSet, attributeName, attributes[attributeName])

    return newPSet

def Service(serviceName, **kwargs):
    """
    _Service_

    Create a new service.
    """
    newService = Container()
    setattr(newService, "serviceName", serviceName)
    return newService
