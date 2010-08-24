#!/bin/env python
#pylint: disable-msg=E1103
# E1103: The thread will have a logger and a dbi before it gets here

# It's the tracker for BossLite





import logging
import time
import threading

from WMCore.DAOFactory import DAOFactory

from WMComponent.JobTracker.Plugins.TrackerPlugin  import TrackerPlugin

from WMCore.BossLite.DbObjects.Job           import Job
from WMCore.BossLite.DbObjects.RunningJob    import RunningJob
from WMCore.BossLite.DbObjects.BossLiteDBWM  import BossLiteDBWM
#from WMCore.WMConnectionBase    import WMConnectionBase



class BossLiteTracker(TrackerPlugin):
    """
    _BossLiteTracker_

    Pulls job info directly out of BossLite
    """



    def __init__(self, config):

        TrackerPlugin.__init__(self, config)
        self.classAds = None
        self.config = config

        myThread = threading.currentThread()

        #self.engine = WMConnectionBase(daoPackage = "WMCore.BossLite")

        self.daoFactory = DAOFactory(package = "WMCore.BossLite",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        return


    def __call__(self, jobDict):

        #self.getClassAds()
        trackDict = self.track(jobDict)

        return trackDict



    def track(self, jobDict):
        """
        _track_

        Do the comparison between the jobs we're looking for
        and the classAds we have
        """

        logging.info('Getting BossLite jobs')

        # Create an object to store final info
        trackDict = {}

        # Create an object to store the jobs to kill
        killList  = []

        # Get all the jobs in BossLite
        jobAction = self.daoFactory(classname = "Job.LoadJobRunningJob")
        jobList   = jobAction.execute()

        # Reformat jobList
        # TODO: Think of a better way to do this
        jobInfo = {}
        for job in jobList:
            jobInfo[job['wmbsJobId']] = job

        
        logging.info('Translating BossLite job status')


        # Go over each job in WMBS
        for job in jobDict:
            # If we don't have the job, then it's finished
            # This is for the accountant to deal with
            if not job['id'] in jobInfo.keys():
                trackDict[job['id']] = {'Status': 'NA', 'StatusTime': -1}
            else:
                jobAd     = jobInfo.get(job['id'])
                jobStatus = jobAd.get('status', 'C')
                statName  = 'NA'
                #dtStamp   = jobAd.get('lbTimestamp')
                #jobTime   = time.mktime(dtStamp.timetuple())
                jobTime   = jobAd.get('lbTimestamp')

                if jobStatus is None:
                    logging.error("None job status for '%s'"%str(jobAd))
                    continue

                # If the job is waiting to run, it's Idle
                if jobStatus.lower() in ['c', 'su', 'w', 'ss', 'sr']:
                    statName = 'Idle'
                # If the job is running, it's running
                elif jobStatus.lower() == 'r':
                    statName = 'Running'
                # If the job is in the process of finished, we wait for it
                elif jobStatus.lower() in ['sd', 'a', 'k']:
                    statName = 'Finishing'
                # If the job is done, we dump it
                elif jobStatus.lower() == 'e':
                    statName = 'NA'
                    # Since this job is done, we get rid of it
                    killList.append(jobAd['name'])
                # Otherwise, we've got nothing
                else:
                    statName = jobStatus

                trackDict[job['id']] = {'Status': statName,
                                        'StatusTime': long(time.time()) - jobTime}
            
        # At the end, kill all the jobs that have exited
        logging.info("Removing exited jobs")
        self.kill(killList = killList)

        logging.info("Done!")

        return trackDict



    def kill(self, killList):
        """
        Kill a list of jobs based on the 

        """

        db = BossLiteDBWM()

        for name in killList:
            job = Job(parameters = {'name': name})
            job.load(db)
            job.remove(db)

        return

