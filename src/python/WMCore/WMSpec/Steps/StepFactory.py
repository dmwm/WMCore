#!/usr/bin/env python
"""
_StepFactory_

Factory implementation to retrieve Step Template instances

"""


from WMCore.WMFactory import WMFactory


class TemplateFactory(WMFactory):
    """
    _TemplateFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Steps.Templates")

class BuilderFactory(WMFactory):
    """
    _BuilderFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Steps.Builders")

class ExecutorFactory(WMFactory):
    """
    _ExecutorFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Steps.Executors")
class EmulatorFactory(WMFactory):
    """
    _EmulatorFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Steps.Emulators")



_TemplateFactory = TemplateFactory()
_BuilderFactory = BuilderFactory()
_ExecutorFactory = ExecutorFactory()
_EmulatorFactory = EmulatorFactory()



def getStepTemplate(stepType):
    """
    _getStepTemplate_

    factory method to return a step template instance based on the
    name of the step

    """
    return _TemplateFactory.loadObject(stepType)


def getStepBuilder(stepType):
    """
    _getStepBuilder_

    """
    return _BuilderFactory.loadObject(stepType)

def getStepExecutor(stepType):
    """
    _getStepExecutor_

    Get an Executor for the given step type

    """
    return _ExecutorFactory.loadObject(stepType)

def getStepEmulator(stepEmuName):
    """
    _getStepEmulator_

    Get an instance of a given emulator, Note that this takes the name
    of the emulator rather than the step Type since there are multiple
    ways to emulate a given step

    """
    return _EmulatorFactory.loadObject(stepEmuName)
