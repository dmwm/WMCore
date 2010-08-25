#!/bin/env python

#Basic model for a tracker plugin
#Should do nothing

__revision__ = "$Id: TestTracker.py,v 1.2 2009/11/06 20:36:56 mnorman Exp $"
__version__ = "$Revision: 1.2 $"


import logging
import os
import string

from xml.sax.handler import ContentHandler
from xml.sax import make_parser


#from ResourceMonitor.Monitors.CondorQ import condorQ

from WMComponent.JobTracker.Plugins.TrackerPlugin  import TrackerPlugin
from WMComponent.JobTracker.Plugins.CondorQHandler import CondorQHandler


class TestTracker(TrackerPlugin):


    def __init__(self, config):

        self.classAds = None

        return


    def __call__(self, jobDict):

        self.getClassAds()
        trackDict = self.track(jobDict)

        return trackDict

    def getClassAds(self):

        constraint = "\"WMAgent_JobID =!= UNDEFINED\""


        command = "condor_q -xml -constraint %s  " % constraint
        logging.debug("condorQ command: '%s'"%command)
        si, sout = os.popen2(command)
        content = sout.read()

        #  // If we get invalid xml from condor_q the line below will throw an
        # //  exception -- we want that to happen -- the component catches it
        #//   and that fact is used to distinguish an error from there being no jobs
    
        content = content.split("<!DOCTYPE classads SYSTEM \"classads.dtd\">")[1]
        
        handler = CondorQHandler()
        parser = make_parser()
        parser.setContentHandler(handler)

        try:
            parser.feed(content)
        except Exception, ex:
            # No xml data, no override, nothing to be done...
            return 
        
        classAdsRaw = handler.classads
        classAds = {}
        #Format classAds
        #Use ProdAgent_JobID for now
        for ad in classAdsRaw:
            if not 'WMAgent_JobID' in ad.keys():
                continue
            classAds[int(ad['WMAgent_JobID'])] = ad
        self.classAds = classAds
        logging.info("Retrieved %s Classads" % len(self.classAds))


        return


    def kill(self, killList):
        """
        Kill a list of jobs based on the WMBS job names

        """
        listToKill = []

        #Should already HAVE the ClassAds
        #self.getClassAds()
        command = "condor_rm"
        for name in killList:
            command = '%s %s' %(command, str(self.classAds[name]['ClusterId']))
        
        #Now kill 'em
        logging.debug("condor_rm command: '%s'"%command)
        si, sout = os.popen2(command)
        content = sout.read()        

        return


    def track(self, jobDict):
        """
        This actually does all the work of searching for jobs in the classAds
        """

        trackDict = {}
        
        for job in jobDict:
            #Jobs should be of the format {'name': name, 'location': location}
            if not job['id'] in self.classAds.keys():
                trackDict[job['id']] = {'Status': 'NA', 'StatusTime': -1, 'StatusReason': -1}
            elif self.classAds[job['id']]['JobStatus'] == 5:
                #Job is Held
                trackDict[job['id']] = {'Status': 'Held', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['name']]['EnteredCurrentStatus'], \
                                        'StatusReason': self.classAds[job['name']]['HoldReason']}
            elif self.classAds[job['id']]['JobStatus'] == 1:
                #Job is Idle
                trackDict[job['id']] = {'Status': 'Idle', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['name']]['EnteredCurrentStatus'], \
                                        'StatusReason': self.classAds[job['name']]['LastRejMatchReason']}
            elif self.classAds[job['id']]['JobStatus'] == 2:
                #Job is Running
                trackDict[job['id']] = {'Status': 'Running', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['name']]['EnteredCurrentStatus'], \
                                        'StatusReason': -1}
            else:
                #Job is in some strange state
                trackDict[job['id']] = {'Status': 'Unknown', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['name']]['EnteredCurrentStatus'], \
                                        'StatusReason': self.classAds[job['name']]['JobStatus'] }


        return trackDict









