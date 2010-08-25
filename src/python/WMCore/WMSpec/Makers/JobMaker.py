#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902

"""
Makes jobs in the proper state.
"""
__revision__ = "$Id: JobMaker.py,v 1.1 2009/06/19 18:12:07 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import logging

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory



class JobMaker(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
	print "JobMaker.__init__"

    def preInitialization(self):
	print "JobMaker.preInitialization"

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['MakeJob'] = \
            factory.loadObject('WMCore.WMSpec.Makers.Handlers.MakeJob', self)
