#!/usr/bin/env python
"""
DBS Buffer handler for JobSuccess event
"""
__all__ = []

__revision__ = "$Id: JobSuccess.py,v 1.1 2008/10/02 19:57:14 afaq Exp $"
__version__ = "$Reivison: $"
__author__ = "anzar@fnal.gov"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
import cPickle

class JobSuccess(BaseHandler):
    """
    Default handler for create failures.
    """


    """
    def __init__(self):
	BaseHandler.__init__(self)
	print "THIS is Called"
    """


    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.

	#print "I am not sure about thread pools here"

        #self.threadpool = ThreadPool(\
        #    "WMComponent.DBSBuffer.Handler.DefaultRunSlave", \
        #    self.component, 'JobSuccess', \
        #    self.component.config.DBSBuffer.maxThreads)

        # this we overload from the base handler



    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.

	print event, payload
	print event + " ::::::: Handled"

	fjrPickle = open("fjr.pck", 'w')
	cPickle.dump(str(open(payload, 'r').read()), fjrPickle)
	fjrPickle.close()


        #self.threadpool.enqueue(event, payload)


