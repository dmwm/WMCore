'''
Created on Jul 31, 2014

@author: sryu
'''
from __future__ import print_function, division
from builtins import object

import cherrypy
import traceback
from threading import Thread, Condition

from WMCore.WMLogging import getTimeRotatingLogger


class CherryPyPeriodicTask(object):

    def __init__(self, config):

        """
        BaseClass which can set up the concurrent task using cherrypy thread.
        WARNING: This assumes each task doesn't share the object.
        (It can be share only read operation is performed)
        If the object shared by multple task and read/write operation is performed.
        Lock is not provided for these object

        :arg config  WMCore.Configuration object. which need to contain in duration attr.
        TODO: add validation for config.duration
        """
        self.logger = getTimeRotatingLogger(config._internal_name, config.log_file)
        self.logger.info("Setting CherryPy periodic task with the following config:\n%s", config)
        self.setConcurrentTasks(config)
        self.setUpLogDB(config)

        for task in self.concurrentTasks:
            PeriodicWorker(task['func'], config, task['duration'], logger=self.logger, logDB=self.logDB)

    def setUpLogDB(self, config):
        if hasattr(config, "central_logdb_url"):
            # default set up for logDB config section need to contain propervalues
            from WMCore.Services.LogDB.LogDB import LogDB
            self.logDB = LogDB(config.central_logdb_url, config.log_reporter, logger=self.logger,
                               thread_name=config.object.rsplit(".", 1)[-1])
        else:
            self.logDB = None

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks,
        sub class should implement this

        each function in the list should have the same signature with
        3 arguments (self, config, duration)
        config is WMCore.Configuration object
        """
        self.concurrentTasks = {'func': None, 'duration': None}
        raise NotImplementedError("need to implement setSequencialTas assign self._callSequence")

class PeriodicWorker(Thread):

    def __init__(self, func, config, duration=600, logger=cherrypy.log, logDB=None):
        # use default RLock from condition
        # Lock wan't be shared between the instance used  only for wait
        # func : function or callable object pointer
        self.wakeUp = Condition()
        self.stopFlag = False
        self.taskFunc = func
        self.config = config
        self.duration = duration
        self.logger = logger
        self.logDB = logDB

        try:
            name = func.__name__
            print(name)
        except AttributeError:
            name = func.__class__.__name__
            print(name)

        Thread.__init__(self, name=name)
        cherrypy.engine.subscribe('start', self.start, priority=100)
        cherrypy.engine.subscribe('stop', self.stop, priority=100)

    def stop(self):
        self.wakeUp.acquire()
        self.stopFlag = True
        self.wakeUp.notifyAll()
        self.wakeUp.release()

    def run(self):

        while not self.stopFlag:
            self.wakeUp.acquire()
            try:
                self.taskFunc(self.config)
                self.heartBeatInfoToLogDB()
            except Exception as e:
                self.logger.error("Periodic Thread ERROR %s.%s %s"
                % (getattr(e, "__module__", "__builtins__"),
                e.__class__.__name__, str(e)))
                traceMsg = traceback.format_exc()
                for line in traceMsg.rstrip().split("\n"):
                    self.logger.error(" " + line)
                self.heartBeatErrorToLogDB(traceMsg)

            self.wakeUp.wait(self.duration)
            self.wakeUp.release()

    def heartBeatInfoToLogDB(self):
        if self.logDB:
            self.logDB.delete(mtype="error", this_thread=True, agent=False)
            self.logDB.post(mtype="info")
        return

    def heartBeatErrorToLogDB(self, msg):
        if self.logDB:
            self.logDB.post(msg=msg, mtype="error")
        return


class SequentialTaskBase(object):

    """
    Base class for the tasks which should run sequentially
    """
    def __init__(self):
        self.setCallSequence()

    def __call__(self, config):
        for call in self._callSequence:
            try:
                call(config)
            except Exception as ex:
                #log the excpeiotn and break.
                #SequencialTasks are interconnected between functions
                trace = traceback.format_exc()
                self.logger.error("%s:\n %s" % (str(ex), trace))
                break

    def setCallSequence(self):
        """
        set the list of function call with out args on self.callSequence

        i.e.
        self.callSequence = [self.do_something1, self.do_something1]
        """
        raise NotImplementedError("need to implement setCallSequence assign self._callSequence")


#this is the sckeleton of request data collector
class DataUploadTask(SequentialTaskBase):

    def setCallSequence(self):
        self._callSequence = [self.getData, self.convertData, self.putData]

    def getData(self, config):
        # self.data = getData(self.sourceUrl)
        pass

    def convertData(self, config):
        # self.data = convertData(self.data)
        pass

    def putData(self, config):
        # putData(self.destUrl)
        pass
