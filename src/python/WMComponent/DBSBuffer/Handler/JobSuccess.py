#!/usr/bin/env python
"""
Default handler for Job Success events.
"""
__all__ = []
__revision__ = "$Id: JobSuccess.py,v 1.15 2009/05/15 16:54:51 mnorman Exp $"
__version__ = "$Revision: 1.15 $"


from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

import exceptions
import threading


#TODO: InvalidJobReport will come from DBSInterface or elsewhere
class InvalidJobReport(exceptions.Exception):
  def __init__(self,jobreportfile):
   args="Invalid JobReport file: %s\n"%jobreportfile
   exceptions.Exception.__init__(self, args)
   pass

  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)

  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)

class JobSuccess(BaseHandler):
    """
    Default handler for Job Success Events.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # define a slave threadpool (this is optional
        # and depends on the developer deciding how he/she
        # wants to implement certain logic.
        self.threadpool = ThreadPool(\
            "WMComponent.DBSBuffer.Handler.JobSuccessSlave", \
            self.component, 'JobSuccess', \
            1)
        myThread = threading.currentThread()
        myThread.msgService.purgeMessages()

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # as we defined a threadpool we can enqueue our item
        # and move to the next.
        print "Thread with payload " + str(payload) + " is enqueued"
        self.threadpool.enqueue(event, payload)
