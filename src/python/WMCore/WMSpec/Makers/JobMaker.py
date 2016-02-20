#!/usr/bin/env python
#pylint: disable=E1101,E1103,C0103,R0902

"""
Makes jobs in the proper state.
"""
from __future__ import print_function




import logging

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory



class JobMaker(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        print("JobMaker.__init__")

    def preInitialization(self):
        print("JobMaker.preInitialization")

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['MakeJob'] = \
            factory.loadObject('WMCore.WMSpec.Makers.Handlers.MakeJob', self)
