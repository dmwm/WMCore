#!/usr/bin/env python

"""
_TestComponent_

Compnent to test the skeleton and serve as an example to build
components.


"""





import logging

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness

# just the base handler for some tests.
from WMCore.Agent.BaseHandler import BaseHandler

class TestComponent(Harness):
    """
    _TestComponent_

    Compnent to test the skeleton and serve as an example to build
    components.

    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)

    def logState(self):
        """
        Augment standard logging message.
        """

        state = 'This is the state of the TestComponent\n'
        state += '-------------------------------------'
        return state

    def preInitialization(self):
        """
        Override pre initialization.
        """
        aHandlerInstance = BaseHandler(self)
        # obviously this does not have to be a 1 on 1 mapping.
        # we can have one instance for several messages
        self.messages['TestMessage1'] = aHandlerInstance
        self.messages['TestMessage2'] = aHandlerInstance
        # or multiple instances for different messages.
        self.messages['TestMessage3'] = BaseHandler(self)
        self.messages['TestMessage4'] = BaseHandler(self)

        logging.debug("TestComponent pre initialization")

    def postInitialization(self):
        """
        Override post initialization.
        """

        logging.debug("TestComponent post initialization")
