#!/usr/bin/env python
"""
_GeneratorFactory_


"""

from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException

class GeneratorFactoryException(WMException):
    """
    _GeneratorFactoryException_

    Exception for missing objects or problems

    """
    pass


class GeneratorFactory(WMFactory):
    """
    _GeneratorFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.JobSplitting.Generators")


_Factory = GeneratorFactory()


def getGenerator(generatorName, wmTaskHelper, **options):
    """
    _getGenerator_

    factory method to return a step template instance based on the
    name of the step

    """
    args = {}
    args.update(options)
    args['task'] = wmTaskHelper
    try:
        return _Factory.loadObject(generatorName,
                                   args)
    except WMException, wmEx:
        msg = "GeneratorFactory Unable to load Object: %s" % generatorName
        raise GeneratorFactoryException(msg)
    except Exception, ex:
        msg = "Error creating object %s in GeneratorFactory:\n" % generatorName
        msg += str(ex)
        raise GeneratorFactoryException(msg)


def makeGenerators(wmTaskHelper):
    """
    _makeGenerators_

    Util function to build the set of generators defined
    by the task provided

    """
    result = []
    for generator in wmTaskHelper.listGenerators():
        genInstance = getGenerator(
            generator,
            wmTaskHelper, **wmTaskHelper.getGeneratorSettings(generator)
            )
        result.append(genInstance)
    return result
