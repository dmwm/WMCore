'''
Created on Aug 13, 2014

@author: sryu
'''
import cherrypy

from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask

class LogDBTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.centralLogDBUpdate, 'duration': config.logDBUpdateDuration},
                                {'func': self.logDBCleanUp, 'duration': config.logDBCleanDuration}]

    def centralLogDBUpdate(self, config):
        """
        gather active data statistics
        """
        
        logdb = LogDB(config.logdb_url, config.log_reporter, 
                      config.central_logdb_url)
        
        requests = logdb.get_all_requests()
        for req in requests:
            logdb.upload2central(req)
            
        cherrypy.log("total %s requests updated" % len(requests))        
        return
    
    def logDBCleanUp(self, config):
        """
        gather active data statistics
        """
        
        logdb = LogDB(config.logdb_url, config.log_reporter, 
                      config.central_logdb_url)
        logdb.cleanup(config.logDBCleanDuration)
        
        cherrypy.log("cleaned up log db")        
        return
