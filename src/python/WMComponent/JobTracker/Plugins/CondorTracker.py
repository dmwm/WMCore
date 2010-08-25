#!/bin/env python

#Basic model for a tracker plugin
#Should do nothing

__revision__ = "$Id: CondorTracker.py,v 1.1 2010/05/28 16:33:32 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


import logging
import os

import subprocess
import re
import time

#from xml.sax.handler import ContentHandler
from xml.sax import make_parser

from WMComponent.JobTracker.Plugins.TrackerPlugin  import TrackerPlugin
from WMComponent.JobTracker.Plugins.CondorQHandler import CondorQHandler


command = command = ['condor_q', '-format', '\"(JobStatus: \%s)\"  ', 'JobStatus', ' -format', '\"(RejectedReason: \%s)\"  ', 'LastRejMatchReason']


class CondorTracker(TrackerPlugin):
    """
    _CondorTracker_

    Tracks condor jobs using a callout to condor_q
    """



    def __init__(self, config):

        TrackerPlugin.__init__(self, config)
        self.classAds = None

        return


    def __call__(self, jobDict):

        #self.getClassAds()
        trackDict = self.track(jobDict)

        return trackDict


    def getClassAds(self):
        """
        _getClassAds_
        
        Grab classAds from condor_q using xml parsing
        """

        constraint = "\"WMAgent_JobID =!= UNDEFINED\""


        jobInfo = {}

        command = ['condor_q', '-constraint', 'WMAgent_JobID =!= UNDEFINED',
                   '-format', '(JobStatus:\%s)  ', 'JobStatus',
                   '-format', '(stateTime:\%s)  ', 'EnteredCurrentStatus',
                   '-format', '(WMAgentID:\%d):::',  'WMAgent_JobID']
        pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
        stdout, stderr = pipe.communicate()
        classAdsRaw = stdout.split(':::')


        if classAdsRaw == '':
            # We have no jobs
            return jobInfo

        for ad in classAdsRaw:
            # There should be one for every job
            if not re.search("\(", ad):
                # There is no ad.
                # Don't know what happened here
                continue
            statements = ad.split('(')
            tmpDict = {}
            for statement in statements:
                # One for each value
                if not re.search(':', statement):
                    # Then we have an empty statement
                    continue
                key = str(statement.split(':')[0])
                value = statement.split(':')[1].split(')')[0]
                tmpDict[key] = value
            if not 'WMAgentID' in tmpDict.keys():
                # Then we have an invalid job somehow
                logging.error("Invalid job discovered in condor_q")
                logging.error(tmpDict)
                continue
            else:
                jobInfo[int(tmpDict['WMAgentID'])] = tmpDict

        logging.info("Retrieved %i classAds" % len(jobInfo))


        return jobInfo


    def track(self, jobDict):
        """
        _track_

        Do the comparison between the jobs we're looking for
        and the classAds we have
        """

        # Create an object to store final info
        trackDict = {}

        # Get the job
        jobInfo = self.getClassAds()

        for job in jobDict:
            # Now go over the jobs from WMBS and see what we have
            if not job['id'] in jobInfo.keys():
                trackDict[job['id']] = {'Status': 'NA', 'StatusTime': -1}
            else:
                jobAd     = jobInfo.get(job['id'])
                jobStatus = int(jobAd.get('JobStatus', 0))
                statName  = 'NA'
                if jobStatus == 1:
                    # Job is Idle, waiting for something to happen
                    statName = 'Idle'
                elif jobStatus == 5:
                    # Job is Held; experienced an error
                    statName = 'Held'
                elif jobStatus == 2:
                    # Job is Running, doing what it was supposed to
                    statName = 'Running'
                else:
                    # Then we have no clue
                    statName = 'Unknown'
                trackDict[job['id']] = {'Status': statName,
                                        'StatusTime': time.time() - \
                                        float(jobAd.get('stateTime', 0))}


        return trackDict



    def kill(self, killList):
        """
        Kill a list of jobs based on the WMBS job names

        """

        for jobID in killList:
            # This is a very long and painful command to run
            command = 'condor_rm -constraint \"WMAgent_JobID =?= %i\"' % (jobID)
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE, shell = True)
            out, err = proc.communicate()

        return
