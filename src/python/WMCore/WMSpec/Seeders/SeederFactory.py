#!/usr/bin/env python
"""
_SeederFactory_



"""

from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException


class SeederFactoryException(WMException):
    """
    _SeederFactortyException_

    Exception for missing objects or problems

    """
    pass

class SeederFactory(WMFactory):
    """
    _SeederFactory_

    Instantiate a WMFactory instance with the appropriate namespace

    """
    def __init__(self):
        WMFactory.__init__(self, self.__class__.__name__,
                           "WMCore.WMSpec.Seeders")



_SeederFactory = SeederFactory()


def getSeeder(seederName, **args):
    """
    _getSeeder_

    Instantiate the named seeder passing through the argument dict
    to the Seeder ctor

    """
    try:
        return _SeederFactory.loadObject(seederName, args)
    except WMException, wmEx:
        msg = "SeederFactory Unable to load Object: %s" % seederName
        raise StepFactoryException(msg)
    except Exception, ex:
        msg = "Error creating object %s in SeederFactory:\n" % seederName
        msg += str(ex)
        raise SeederFactoryException(msg)




