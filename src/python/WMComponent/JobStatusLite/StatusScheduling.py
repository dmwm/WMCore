#!/usr/bin/env python
"""
The actual JobStatus process scheduling algorithm
"""
__all__ = []
__revision__ = "$Id: StatusScheduling.py,v 1.1 2010/05/13 15:55:49 mcinquil Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import logging
import traceback
from sets import Set

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.DAOFactory                     import DAOFactory
from WMCore.ProcessPool.ProcessPool        import ProcessPool

from WMCore.BossLite.DbObjects.StatusDB    import StatusDB
from WMComponent.JobStatusLite.JobStatusWork   import JobStatusWork

class StatusScheduling(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.config = config
        #self.config.JobStatusLite.maxJobsCommit
        #self.config.JobStatusLite.maxJobQuery
        #self.config.JobStatusLite.processes
        #self.config.JobStatusLite.taskLimit

        myThread = threading.currentThread()

        #self.daoFactory = DAOFactory(package = "WMCore.WMBS",
        #                             logger = myThread.logger,
        #                             dbinterface = myThread.dbi)

        configDict = {'jobtype': "cmssw"}


        self.processPool = ProcessPool( \
                           "JobStatus.StatusWorker", \
                           config.JobStatusLite.processes, \
                           componentDir = self.config.JobStatusLite.componentDir, \
                           config = self.config, slaveInit = configDict \
                         )

        self.groupsUnderProcessing = Set([])
    
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
        pool threads
        """
        myThread = threading.currentThread()

        # Process new jobs
        JobStatusWork.addNewJobs(self.config.JobStatusLite.maxJobsCommit)

        db = StatusDB()

        # apply policy
        groupstemp = self.applyPolicy(db)
        groups = []
        import time
        for i in groupstemp:
            groups.append({i : '%s'%str(time.time())})

        # any job to check?
        if len(groups) == 0:

            # no, wait for jobs to arrive
            logging.info( "No work to do, " + \
                          "scheduler goes to sleep.")
            return

        # new processes to start
        self.processPool.enqueue(groups)

        # wait for processes to finish
        logging.info("Waiting workers...")
        groupback = self.processPool.dequeue(len(groups))
        logging.info("Finished processing workers") 
        JobStatusWork.removeFinishedJobs()
        logging.info("Going to sleep..")

        return

    def applyPolicy(self, tdb):
        """
        __applyPolicy__

        apply policy.
        """

        # set policy parameters
        groups = {}

        # get list of groups under processing
        grlist = ",".join(["%s" % k for k in self.groupsUnderProcessing])

        # get information about tasks associated to these groups
        jobPerTask = tdb.getUnprocessedJobs( grlist )

        # process all groups
        grid = 0
        while jobPerTask != [] :

            grid = grid + 1
            ntasks = 0

            # ignore groups under processing
            if grid in self.groupsUnderProcessing:
                logging.info( "skipping group " + str(grid))
                continue

            # build group information
            groups[grid] = ''
            jobsReached = 0

            logging.debug('filling group ' + str(grid) + ' with largest tasks')

            # fill group with the largest tasks
            while jobPerTask != [] and ntasks < self.config.JobStatusLite.taskLimit:
                try:

                    task, jobs = jobPerTask[0]

                    # stop when there are enough jobs
                    totreached = jobsReached + int(jobs)
                    if totreached > self.config.JobStatusLite.maxJobQuery \
                           and jobsReached != 0:
                        break

                    # add task to group
                    groups[grid] += str(task) + ','
                    jobsReached += int(jobs)
                    jobPerTask.pop(0)

                    # stop when there are too much tasks
                    ntasks += 1

                # go to next task
                except IndexError, ex:
                    jobPerTask.pop(0)
                    logging.info("\n\n" + str(ex) + "\n\n")
                    continue

            logging.debug('filling group ' + str(grid) + \
                          ' with the smallest tasks')

            # fill group with the smallest tasks
            while jobPerTask != [] and ntasks < 30 :
                try:
                    task, jobs = jobPerTask[0]

                    # stop when there are enough jobs
                    totreached = jobsReached + int(jobs)
                    if totreached > self.config.JobStatusLite.maxJobQuery:
                        break

                    # add task to group
                    groups[grid] += task + ','
                    jobsReached += int(jobs)
                    jobPerTask.pop()

                    # stop when there are too much tasks
                    ntasks += 1

                # go to next task
                except IndexError:
                    jobPerTask.pop()
                    continue

            logging.info("group " + str(grid) + " filled with tasks " \
                          + groups[grid] + " and total jobs " \
                          + str(jobsReached))

        del jobPerTask[:]
        # process all groups
        for group, tasks in groups.iteritems():

            # ignore empty tasks
            if tasks == '':
                continue

            # update group
            tdb.setTaskGroup( str(group), tasks[:-1] )
            logging.debug("Adding tasks " + tasks[:-1] + ' to group ' + \
                          str(group))

        # build list of groups
        ret = groups.keys()
        ret.extend(self.groupsUnderProcessing)

        logging.info("returning groups " + ret.__str__())

        # and return it
        return ret

