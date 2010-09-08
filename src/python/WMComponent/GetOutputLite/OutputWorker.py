#!/usr/bin/env python
"""
The GetOutput subprocess worker for the output/postmortem processing
"""

import threading
import logging
import os

#from WMCore.DAOFactory                      import DAOFactory
from WMCore.Agent.Configuration             import Configuration
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI


class OutputWorker:
    """
    The JobStatus subprocess worker for the status check/update
    """

    def __init__(self, **configDict):
        """
        init OutputWorker
        """

        config = Configuration()
        self.config = config

        return

    def __call__(self, parameters):
        """
        __call__

        processing input groups
        """
  
        mythread = threading.currentThread()

        logging.debug(str(mythread) + ": in OutputWorker.__call__")
        logging.debug("Params: " + str(parameters) )


        for work in parameters:
            logging.info('Working on [%s]...'%str(work['taskId']))
            if work['taskId'] is None:
                return {}

            logging.info('Loading task')
            bossSession = BossLiteAPI()
            task = bossSession.loadTask( taskId = work['taskId'],
                                         jobRange = work['joblist'])
            if task['user_proxy'] is None :
                task['user_proxy'] = ''

            ## HARD CODING GLITE -> needed a configuration parameter somewhere
            schedulerconfig = { 'timeout' : len( task.jobs ) * 30,
                                'name': 'SchedulerGLite',
                                'user_proxy': task['user_proxy'] }
            try:
                schedSession = BossLiteAPISched( bossSession,
                                                 schedulerconfig,
                                                 task )


                ## before retrieving succeeded jobs..
                if work['status'] == 'SD':

                    ## start retrieving
                    logging.info('Retrieving output for ' + 
                                 '%i jobs'%len(work['joblist']))
                    logging.info(' for jobs %s'%str(work['joblist']))
                    task = schedSession.getOutput(
                                             taskObj = task,
                                             jobRange = work['joblist'],
                                                 )
                ## then retrieving aborted jobs!
                elif work['status'] == 'A':

                    ## start retrieving (by default in job out dir)
                    logging.info('Retrieving post mortem information for ' +
                                 '%i jobs, %s'%(len(work['joblist']),str(work['joblist'])))
                    schedSession.postMortem( taskObj = task,
                                             jobRange = work['joblist'] )
                    
            except Exception, ex:
                logging.error("Problem [%s]"%str(ex))
                import traceback
                logging.error(str(traceback.format_exc()))
            logging.info('Done') 

        logging.debug(str(mythread) + ": finished OutputWorker.__call__")
        logging.debug("Returning [%s]"%str(parameters))
        
        return {}

