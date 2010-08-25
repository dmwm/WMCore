#!/bin/env python

# It's the tracker for BossLite

__revision__ = "$Id: BossLiteTracker.py,v 1.1 2010/07/08 15:44:57 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


import logging
import os
import time
import threading

from WMCore.DAOFactory import DAOFactory

#from xml.sax.handler import ContentHandler
from WMComponent.JobTracker.Plugins.TrackerPlugin  import TrackerPlugin

from WMCore.BossLite.DbObjects.Job           import Job
from WMCore.BossLite.DbObjects.BossLiteDBWM  import BossLiteDBWM




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

        # Create an object to store final info
        trackDict = {}

        # Create an object to store the jobs to kill
        killList  = []

        # Get all the jobs in BossLite
        jobAction = self.daoFactory(classname = "RunningJob.LoadJobStatus")
        jobList   = jobAction.execute()

        # Reformat jobList
        # TODO: Think of a better way to do this
        jobInfo = {}
        for job in jobList:
            jobInfo[job['id']] = job


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
                dtStamp   = jobAd.get('time')
                jobTime   = time.mktime(dtStamp.timetuple())
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
                                        'StatusTime': time.time() - jobTime}
            

        # At the end, kill all the jobs that have exited
        self.kill(killList = killList)


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
