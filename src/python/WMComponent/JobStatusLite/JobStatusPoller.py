#!/usr/bin/env python
"""
The actual JobStatusLite poller algorithm
"""
__all__ = []



import threading
import logging
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

#from WMCore.DAOFactory        import DAOFactory
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.DbObjects.StatusDB import StatusDB

from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

#from WMCore.BossLite.API.TrackingAPI      import TrackingAPI


class JobStatusPoller(BaseWorkerThread):
    """
    Polls for jobs in new process_status or to notify as finished
    """

    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.config = config
        #self.config.JobStatusLite.maxJobQuery 

        self.myThread = threading.currentThread()
        logging.info("Thread: [%s]" %str(self.myThread ))

        #self.daoFactory = DAOFactory(package = "WMCore.WMBS",
        #                             logger = self.myThread.logger,
        #                             dbinterface = self.myThread.dbi)

        self.newAttrs = { 'processStatus' : 'not_handled',
                          'closed' : 'N' }
        self.failedAttrs = { 'processStatus' : 'handled',
                             'status' : 'A', 'closed' : 'N' }
        self.killedAttrs = { 'processStatus' : 'handled',
                             'status' : 'K', 'closed' : 'N' }
        self.finishedAttrs = { 'processStatus' : 'handled',
                               'status' : 'SD', 'closed' : 'N' }

        self.counters = ['pending', 'submitted', 'waiting', 'ready', \
                         'scheduled', 'running', 'cleared', 'created', 'other']

    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        return

    def terminate(self, params):
        """
        _terminate_

        Terminate the function after one more run.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return

    def algorithm(self, parameters):
        """
	Performs the archiveJobs method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Thread [%s]"%str(self.myThread))

        bliteapi = BossLiteAPI()
        trackdb  = StatusDB()

        try:
            #myThread.transaction.begin()
            self.getStatistic(trackdb)
            #myThread.transaction.commit()
        except Exception, ex:
            #myThread.transaction.rollback()
            logging.error("Problem showing statistics: '%s'."%str(ex))

        try:
            logging.info( 'Load Finished Jobs' )
            #myThread.transaction.begin()
            self.pollJobs( \
                           bliteapi, \
                           self.config.JobStatusLite.maxJobQuery, \
                           self.finishedAttrs, \
                           'output_requested', \
                           []
                         )
            #myThread.transaction.commit()
        except Exception, ex: 
            #myThread.transaction.rollback()
            logging.error("Problem processing finished jobs: '%s'."%str(ex))

        try:
            # get jobs and handle them
            logging.info( 'Load Failed Jobs' )
            #myThread.transaction.begin()
            self.pollJobs(  \
                           bliteapi, \
                           self.config.JobStatusLite.maxJobQuery, \
                           self.failedAttrs, \
                           'failed', \
                           []
                         )
            #myThread.transaction.commit()
        except Exception, ex:
            #myThread.transaction.rollback()
            logging.error("Problem processing failed jobs: '%s'."%str(ex))

        try:
            logging.info( 'Load Killed Jobs' )
            #myThread.transaction.begin()
            self.pollJobs(  \
                           bliteapi, \
                           self.config.JobStatusLite.maxJobQuery, \
                           self.killedAttrs, \
                           'failed', \
                           []
                         )
            #myThread.transaction.commit()
        except Exception, ex:
            #myThread.transaction.rollback()
            logging.error("Problem processing killed jobs: '%s'."%str(ex))

        try:
            logging.info( 'Load New Jobs' )
            #myThread.transaction.begin()
            self.pollJobs( \
                           bliteapi, \
                           self.config.JobStatusLite.maxJobQuery, \
                           self.newAttrs, \
                           'handled', [] ) #, \
#                           ['C', 'S'] \
#                         )
            #myThread.transaction.commit()
        except Exception, ex:
            #myThread.transaction.rollback()
            logging.error("Problem processing new jobs: '%s'."%str(ex))

        return


    def pollJobs(self, bliteapi, jobslimit, runningAttrs, \
                       processStatus, skipStatus ):
        """
        __pollJobs__

        basic structure for jobs polling

        """

        offset  = -1
        loop    = True
        newjobs = []

        while loop:

            logging.debug("Max jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + jobslimit) ) )

            #logging.info(str(runningAttrs))
            #logging.info(str([offset,jobslimit]))

            try:
                #self.myThread.transaction.begin()
                newjobs = bliteapi.loadJobsByRunningAttr( \
                           binds = runningAttrs, \
                           limit = [offset, jobslimit] \
                          )
                #self.myThread.transaction.commit()
            except Exception, ex:
                #self.myThread.transaction.rollback()
                logging.error(
                  "Failed handling %s loaded jobs, waiting next round: %s"
                  % ( processStatus, str( ex ) ) )
                logging.error(str(ex))
                continue

            logging.info("Polled jobs : " + str( len(newjobs) ) )

            # exit if no more jobs to query
            if newjobs == [] :
                loop = False
                break
            else :
                offset = self.getMaxId(newjobs)

            try:
                missingrunjob = []
                #self.myThread.transaction.begin()
                for jj in newjobs:
                    logging.info("Modifying job [%s] with running job [%s]" \
                                 %(str(jj.data['id']),str(jj.runningJob['id'])))
                    if jj.runningJob is None:
                        missingrunjob.append(jj)
                    elif jj.runningJob['status'] not in skipStatus:
                        jj.runningJob['processStatus'] = processStatus
                        jj.runningJob['outputRequestTime'] = int(time.time())
                        jj.runningJob.save(bliteapi.db)
                #self.myThread.transaction.commit()
            except Exception, ex:
                #self.myThread.transaction.rollback()
                logging.error(
                  "Failed handling %s loaded jobs, waiting next round: %s"
                  % ( processStatus, str( ex ) ) )
                logging.error(str(ex))
                continue

            logging.info( "Changed status to %s for %s loaded jobs" \
                              % ( processStatus, str( len(newjobs) ) ) )

            del newjobs[:]
        del newjobs[:]

    def getMaxId(self, joblist):
        """
        _getMaxId_

        Return the max if from a list of bosslite jobs
        """
        maxid = 0
        for job in joblist:
            if job.data['id'] > maxid:
                maxid = job.data['id']
        return maxid

    def getStatistic(self, trackdb):
        """
        __getStatistics__

        Poll the bliteDB for a summary of the job status

        """
        # summary of the jobs in the DB
        result = None
        try:
            #self.myThread.transaction.begin()
            result = trackdb.getJobsStatistic()
            #self.myThread.transaction.commit()
        except Exception, ex:
            #self.myThread.transaction.rollback()
            logging.error("Problem processing statistic: '%s'."%str(ex))

        if result is not None:

            counter = {}
            for ctr in self.counters:
                counter[ctr] = 0

            for pair in result :
                status, count = pair
                if status == 'E':
                    continue
                elif status == 'R' :
                    counter['running'] = count
                elif status == 'I':
                    counter['pending'] = count
                elif status == 'SW' :
                    counter['waiting'] = count
                elif status == 'SR':
                    counter['ready'] = count
                elif status == 'SS':
                    counter['scheduled'] = count
                elif status == 'SU':
                    counter['submitted'] = count
                elif status == 'SE':
                    counter['cleared'] = count
                elif status == 'C':
                    counter['created'] = count
                else:
                    counter['other'] += count

            # display counters
            toprint = "\n\t....................\n"
            for ctr, value in counter.iteritems():
                toprint += "\t%s jobs:\t%s\n" % (ctr, str(value))
            logging.info(toprint + "\t....................\n")

            del( result )

