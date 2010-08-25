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

_TemplateFactory = TemplateFactory()
_BuilderFactory = BuilderFactory()


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

