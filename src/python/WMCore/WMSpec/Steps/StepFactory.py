#!/usr/bin/env python
"""
_StepFactory_

Factory implementation to retrieve Step Template instances

"""


from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException

class StepFactoryException(WMException):
    """
    _StepFactortyException_

    Exception for missing objects or problems

    """
    pass



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

class FetcherFactory(WMFactory):
    """
    _FetcherFactory_

    Instantiate a WMFactory instance for the Fetchers package

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Steps.Fetchers")

class DiagnosticFactory(WMFactory):
    """
    _DiagnosticFactory_

    Instantiate a WMFactory instance for the Diagnostics package

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Steps.Diagnostics")



_TemplateFactory = TemplateFactory()
_BuilderFactory  = BuilderFactory()
_ExecutorFactory = ExecutorFactory()
_EmulatorFactory = EmulatorFactory()
_FetcherFactory  = FetcherFactory()
_DiagnosticFactory = DiagnosticFactory()

def getStepTemplate(stepType):
    """
    _getStepTemplate_

    factory method to return a step template instance based on the
    name of the step

    """
#    try:
    return _TemplateFactory.loadObject(stepType)
#    except WMException, wmEx:
#        msg = "TemplateFactory Unable to load Object: %s" % stepType
#        raise StepFactoryException(msg)
#    except Exception, ex:
#        msg = "Error creating object %s in TemplateFactory:\n" % stepType
#        msg += str(ex)
#        raise StepFactoryException(msg)

def getStepTypeHelper(stepReference):
    """
    _getStepTypeHelper_

    Given a step instance, get its type, use that to get a template
    from the factory and use the template to instantiate the type
    specific helper

    """
    stepType = getattr(stepReference, "stepType", None)
    if stepType == None:
        msg = "Unable to find stepType attribute for step reference passed\n"
        msg += "to getStepTypeHelper method"
        raise StepFactoryException(msg)
    template = getStepTemplate(stepType)
    helper = template.helper(stepReference)
    return helper


def getStepBuilder(stepType):
    """
    _getStepBuilder_

    """
    #try:
    return _BuilderFactory.loadObject(stepType)
#    except WMException, wmEx:
#        msg = "BuilderFactory Unable to load Object: %s" % stepType
#        raise StepFactoryException(msg)
#    except Exception, ex:
#        msg = "Error creating object %s in BuilderFactory:\n" % stepType
#        msg += str(ex)
#        raise StepFactoryException(msg)

def getStepExecutor(stepType):
    """
    _getStepExecutor_

    Get an Executor for the given step type

    """
#    try:
    executor =_ExecutorFactory.loadObject(stepType)
#    except WMException, wmEx:
#        msg = "ExecutorFactory Unable to load Object: %s" % stepType
#        raise StepFactoryException(msg)
#    except Exception, ex:
#        msg = "Error creating object %s in ExecutorFactory:\n" % stepType
#        msg += str(ex)
#        raise StepFactoryException(msg)

    executor.diagnostic = getDiagnostic(stepType)
    return executor

def getStepEmulator(stepEmuName):
    """
    _getStepEmulator_

    Get an instance of a given emulator, Note that this takes the name
    of the emulator rather than the step Type since there are multiple
    ways to emulate a given step

    """
    #try:
    return _EmulatorFactory.loadObject(stepEmuName)
    #except WMException, wmEx:
    #    msg = "EmulatorFactory Unable to load Object: %s" % stepEmuName
    #    raise StepFactoryException(msg)
    #except Exception, ex:
    #    msg = "Error creating object %s in EmulatorFactory:\n" % stepEmuName
    #    msg += str(ex)
    #    raise StepFactoryException(msg)

def getFetcher(fetcherName):
    """
    _getFetcher_

    Get an instance of the named Fetcher implementation

    """
    return _FetcherFactory.loadObject(fetcherName)

def getDiagnostic(stepType):
    """
    _getDiagnostic_

    Get the appropriate Diagnostic for the stepType, falling back
    on a Generic Diagnostic if there isnt a specific impl

    """
    try:
        return _DiagnosticFactory.loadObject(stepType)
    except WMException as wmEx:
        return _DiagnosticFactory.loadObject("Generic")
#    except Exception, ex:
#        msg = "Error creating object %s in DiagnosticFactory:\n" % stepType
#        msg += str(ex)
#        raise StepFactoryException(msg)
