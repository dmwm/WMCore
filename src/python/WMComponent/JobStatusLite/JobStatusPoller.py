#!/usr/bin/env python
"""
The actual JobStatusLite poller algorithm
"""
__all__ = []
__revision__ = "$Id: JobStatusPoller.py,v 1.1 2010/05/13 15:55:47 mcinquil Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import logging
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
#from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.DbObjects.StatusDB import StatusDB
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

        myThread = threading.currentThread()

        #self.daoFactory = DAOFactory(package = "WMCore.WMBS",
        #                             logger = myThread.logger,
        #                             dbinterface = myThread.dbi)

        self.newAttrs = { 'processStatus' : 'not_handled',
                          'closed' : 'N' }
        self.failedAttrs = { 'processStatus' : 'handled',
                             'status' : 'A', 'closed' : 'N' }
        self.killedAttrs = { 'processStatus' : 'handled',
                             'status' : 'K', 'closed' : 'N' }
        self.finishedAttrs = { 'processStatus' : 'handled',
                               'status' : 'SD', 'closed' : 'N' }

    
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
        myThread = threading.currentThread()

        # temporary comment
        bliteapi = None #BossLiteAPI()
        trackdb  = StatusDB()

        # temporary comment
        #if bliteapi is None:
        #    raise Exception("No valid instance of BossLiteAPI!")

        if trackdb is None:
            raise Exception("No valid instance of StatusDB")

        try:
            #myThread.transaction.begin()
            logging.debug(str(myThread))
            
            self.getStatistic(trackdb)

            # temporary comments
#            logging.info( 'Load Finished Jobs' )
#            self.pollJobs( \
#                           trackdb, \
#                           bliteapi, \
#                           self.config.JobStatusLite.maxJobQuery, \
#                           self.finishedAttrs, \
#                           'output_requested' \
#                         )

            # get jobs and handle them
#            logging.info( 'Load Failed Jobs' )
#            self.pollJobs(  \
#                           trackdb, \
#                           bliteapi, \
#                           self.config.JobStatusLite.maxJobQuery, \
#                           self.failedAttrs, \
#                           'failed' \
#                         )
#            logging.info( 'Load Killed Jobs' )
#            self.pollJobs(  \
#                           trackdb, \
#                           bliteapi, \
#                           self.config.JobStatusLite.maxJobQuery, \
#                           self.killedAttrs, \
#                           'failed'
#                         )

            # notify new jobs
#            logging.info( 'Load New Jobs' )
#            self.pollJobs( \
#                           trackdb, \
#                           bliteapi, \
#                           self.config.JobStatusLite.maxJobQuery, \
#                           self.newAttrs, \
#                           'handled', \
#                           ['C', 'S'] \
#                         )

            #myThread.transaction.commit()
        except Exception, ex:
            #myThread.transaction.rollback()
            logging.info(str(ex))
            logging.info( str(traceback.format_exc()) )
            #raise

        return

    def pollJobs(self, trackdb, bliteapi, jobslimit, runningAttrs, \
                       processStatus, skipStatus = None ):
        """
        __pollJobs__

        basic structure for jobs polling

        """

        offset  = 0
        loop    = True
        newjobs = []

        while loop:

            logging.debug("Max jobs to be loaded %s:%s " % \
                         (str( offset ), str( offset + jobslimit) ) )

            #newjobs = bliteapi.loadJobsByRunningAttr(
            #    runningAttrs=runningAttrs, \
            #    limit=jobslimit, offset=offset
            #    )

            logging.info("Polled jobs : " + str( len(newjobs) ) )

            # exit if no more jobs to query
            if newjobs == [] :
                loop = False
                break
            else :
                offset += jobslimit

            try:
                trackdb.processBulkUpdate( newjobs, processStatus, \
                                           skipStatus )
                logging.info( "Changed status to %s for %s loaded jobs" \
                              % ( processStatus, str( len(newjobs) ) ) )

            except Exception, err:
                logging.error(
                    "Failed handling %s loaded jobs, waiting next round: %s" \
                    % ( processStatus, str( err ) ) )
                continue

            del newjobs[:]


    def getStatistic(self, trackdb):
        """
        __getStatistics__

        Poll the bliteDB for a summary of the job status

        """

        # summary of the jobs in the DB
        result = trackdb.getJobsStatistic()
        #track_api = TrackingAPI()
        #result = track_api.getJobsStatistic()
        self.counters = ['pending', 'submitted', 'waiting', 'ready', \
                         'scheduled', 'running', 'cleared', 'created', 'other']

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

