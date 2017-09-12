#!/usr/bin/env python
"""
_GeneratorFactory_


"""

from WMCore.WMException import WMException
from WMCore.WMFactory import WMFactory


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
        self.factory = WMFactory(self.__class__.__name__,
                                 "WMCore.JobSplitting.Generators")

    def getGenerator(self, generatorName, **options):
        """
        _getGenerator_

        factory method to return a step template instance based on the
        name of the step

        """
        args = {}
        args.update(options)
        try:
            return self.factory.loadObject(generatorName,
                                           args)
        except WMException as wmEx:
            msg = "GeneratorFactory Unable to load Object: %s" % generatorName
            raise GeneratorFactoryException(msg)
        except Exception as ex:
            msg = "Error creating object %s in GeneratorFactory:\n" % generatorName
            msg += str(ex)
            raise GeneratorFactoryException(msg)
        return

    def makeGenerators(self, wmTaskHelper):
        """
        _makeGenerators_

        Util function to build the set of generators defined
        by the task provided

        """
        result = []
        for generator in wmTaskHelper.listGenerators():
            genInstance = self.getGenerator(generator, **wmTaskHelper.getGeneratorSettings(generator))
            result.append(genInstance)
        return result
