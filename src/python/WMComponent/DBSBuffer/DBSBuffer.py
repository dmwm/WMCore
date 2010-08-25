#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902

"""
Performs bulk DBS File insertion by buffering, subscribes to job-completion event.
"""




import logging

# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory


#['__call__', '__doc__', '__init__', '__module__', '__str__', 'config', 'handleMessage', 'initInThread', 'initialization', 'logState', 'postInitialization', 'preInitialization', 'prepareToStart', 'publishItem', 'startComponent']

class DBSBuffer(Harness):

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
	print "DBSBuffer.__init__"

    def preInitialization(self):
	print "DBSBuffer.preInitialization"

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['JobSuccess'] = \
            factory.loadObject(self.config.DBSBuffer.jobSuccessHandler, self)



