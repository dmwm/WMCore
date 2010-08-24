#!/usr/bin/env python

"""
_TestComponent_

Compnent to test the skeleton and serve as an example to build 
components.


"""

__revision__ = "$Id: TestComponent.py,v 1.1 2008/08/26 13:55:16 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import logging

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness

# just the base handler for some tests.
from WMCore.Agent.BaseHandler import BaseHandler


class TestComponent(Harness):

    def __init__(self, **args):
        # call the base class
        Harness.__init__(self, **args)


        aHandlerInstance = BaseHandler(self)
        # obviously this does not have to be a 1 on 1 mapping.
        # we can have one instance for several messages
        self.messages['TestMessage1'] = aHandlerInstance
        self.messages['TestMessage2'] = aHandlerInstance
        # or multiple instances for different messages.
        self.messages['TestMessage3'] = BaseHandler(self)
        self.messages['TestMessage4'] = BaseHandler(self)

    def logState(self):
        state = 'This is the state of the TestComponent\n'
        state += '-------------------------------------'
        return state

    def preInitialization(self):
        logging.debug("TestComponent pre initialization")

    def postInitialization(self):
        logging.debug("TestComponent post initialization")




