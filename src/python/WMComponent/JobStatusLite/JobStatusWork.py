#!/usr/bin/env python
"""
_JobStatusWork_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""




#import threading
#from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
#from ProdCommon.BossLite.API.BossLitePoolDB import BossLitePoolDB
#from ProdCommon.BossLite.Common.Exceptions import BossLiteError, TimeOut
#from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
#from ProdCommon.BossLite.Common.Exceptions import SchedulerError
#from ProdCommon.BossLite.Common.System import executeCommand

# Import BossLite DBObjects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob

import traceback
import logging

# Import API
#try:
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched
#except:
#    logging.error("Problem importing BossLiteAPI. Simulating status check.")

from WMCore.BossLite.DbObjects.StatusDB import StatusDB
#from WMCore.BossLite.API.TrackingAPI      import TrackingAPI


###############################################################################
# Class: JobStatusWork                                                        #
###############################################################################

class JobStatusWork:
    """
    A static instance of this class deals with job status operations
    """

    params = {'delay' : 30, 'jobsToPoll' : 200}     # parameters

    def __init__(self):
        """
        Attention: in principle, no instance of this static class is created!
        """
        pass

    @classmethod
    def setParameters(cls, params):
        """
        set parameters
        """
        cls.params = params

    @classmethod
    def addNewJobs(cls, maxvalueinsert = 100):
        """
        include new jobs in the set of jobs to be watched for.

        jobs assigned: all new jobs.

        """
        logging.info("Checking new jobs...")
        db = StatusDB( )
        joblist = db.getUnassociatedJobs()

        # in case of empty results
        if joblist is None or joblist == []:
            logging.info( "No new jobs to be added in query queues")
            return

        countvalueinsert = 0
        multipleinsert = []
        for pair in joblist:
            if countvalueinsert < maxvalueinsert: 
                countvalueinsert += 1
                multipleinsert.append(pair)
            elif countvalueinsert >= maxvalueinsert:
                multipleinsert.append(pair)
                db.addForCheckMultiple(multipleinsert)
                multipleinsert = []
                countvalueinsert = 0
            logging.info(\
                "Adding jobs to queue with id "\
                +  str( pair[0] ) + '.' + str( pair[1] )\
                )
        if len(multipleinsert) > 0:
            db.addForCheckMultiple(multipleinsert)

        del( joblist )

    @classmethod
    def removeFinishedJobs(cls): # , group):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group
        """
        db = StatusDB()
        joblist = db.getAssociatedJobs()

        # in case of empty results
        if joblist is None or joblist == []:
            logging.debug("No finished jobs to be removed from query queues")
            return

        for pair in joblist:
            db.removeFromCheck( pair[0],  pair[1] )

        del( joblist )


    @classmethod
    def doWork(cls, group):
        """
        get the status of the jobs in the group.

        jobs assigned: all jobs in the group.

        """

        logging.info("Getting job status for jobs in group %s" \
                     %str(group) )

        try:
            # get DB sessions
            bossSession = BossLiteAPI()
            #db = StatusDB( bossSession.bossLiteDB )
            db = StatusDB( )
            tasks = db.getGroupTasks(group)

            for taskId in tasks :
                cls.statusQuery( bossSession, int(taskId) )

        except Exception, ex:
            logging.error( "%s exception: %s" \
                           % (str(ex), str( traceback.format_exc() ) ) )


    @classmethod
    def statusQuery( cls, bossSession, taskId ):
        """
        Perform the status query through BossLite objects
        """

        logging.info('Retrieving status for jobs of task %s'  \
                     %str(taskId) )

        # default values
        offset = 0
        loop = True
        runningAttrs = {'processStatus': '%handled',
                        'closed' : 'N'}
        jobsToPoll = int(cls.params['jobsToPoll'])

        # perform query
        while loop :
            ## TODO: improve exception handling
            try :
                #binds = { \
                #         'taskId': taskId, \
                #         'status' :'C' \
                #         }
                binds = { \
                         'taskId': taskId \
                        }
                task = bossSession.loadTask(taskId) #, deep = False)
                logging.info("Loaded task with id %s."%str(taskId))
                selectedJobs = bossSession.loadJobsByRunningAttr( binds, limit = [offset, jobsToPoll] )
                logging.info("Loaded %s jobs."%str(len(selectedJobs)))

                if not len(selectedJobs) > 0 :
                    loop = False
                    break
                else:
                    offset += jobsToPoll

                if task['user_proxy'] is None :
                    task['user_proxy'] = ''

                # Scheduler session
                schedulerConfig = { 'timeout' : len( task.jobs ) * 30, 'name': 'SchedulerGLite', 'user_proxy': task['user_proxy'] }
                
                schedSession = BossLiteAPISched( \
                                                 bossSession, \
                                                 schedulerConfig, \
                                                 task \
                                               )
                
                task = schedSession.query( taskId, queryType='parent' )

                ## printing will slow down performances
                # for job in task.jobs :
                #     print job.runningJob['jobId'], \
                #           job.runningJob['schedulerId'], \
                #           job.runningJob['statusScheduler'], \
                #           job.runningJob['statusReason']

                # log the end of the query
                logging.info('Status retrieved for task %s' \
                             %str(taskId) )
                del task
            except Exception, e:
                logging.error(
                    "Failed to retrieve status for jobs of task %s : %s" \
                    %(str(taskId), str( e ) ) )
                logging.error( traceback.format_exc() )
                loop = False
                offset += int(cls.params['jobsToPoll'])

