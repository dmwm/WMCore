#!/bin/env python

#Basic model for a tracker plugin
#Should do nothing





import logging
import os

#from xml.sax.handler import ContentHandler
from xml.sax import make_parser

from WMComponent.JobTracker.Plugins.TrackerPlugin  import TrackerPlugin
from WMComponent.JobTracker.Plugins.CondorQHandler import CondorQHandler


class TestTracker(TrackerPlugin):
    """
    TestTracker

    Basic class to track jobs passing through a local condor_schedd

    """


    def __init__(self, config):

        TrackerPlugin.__init__(self, config)
        self.classAds = None

        return


    def __call__(self, jobDict):

        self.getClassAds()
        trackDict = self.track(jobDict)

        return trackDict

    def getClassAds(self):
        """
        _getClassAds_
        
        Grab classAds from condor_q using xml parsing
        """

        constraint = "\"WMAgent_JobID =!= UNDEFINED\""


        command = "condor_q -xml -constraint %s  " % constraint
        logging.debug("condorQ command: '%s'" % command)
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
            return ex
        
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

        for jobID in killList:
            # This is a very long and painful command to run
            command = 'condor_rm -constraint \"WMAgent_JobID =?= %i\"' % (jobID)
            proc = Popen(command, stderr = PIPE, stdout = PIPE, shell = True)
            out, err = proc.communicate()

        return

    def purge(self):
        """
        Purge everything in the condor_schedd
        If you're wondering whether or not to use this, DON'T USE IT!

        """

        command = "condor_rm -all"

        sout    = os.popen(command)
        content = sout.read()

        return content


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
                                        - self.classAds[job['id']]['EnteredCurrentStatus'], \
                                        'StatusReason': self.classAds[job['id']]['HoldReason']}
            elif self.classAds[job['id']]['JobStatus'] == 1:
                #Job is Idle
                #logging.error("Job is idle")
                logging.error(self.classAds[job['id']])
                trackDict[job['id']] = {'Status': 'Idle', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['id']]['EnteredCurrentStatus']}
                if self.classAds[job['id']].has_key('LastRejMatchReason'):
                    trackDict[job['id']]['StatusReason'] = self.classAds[job['id']]['LastRejMatchReason']
            elif self.classAds[job['id']]['JobStatus'] == 2:
                #Job is Running
                trackDict[job['id']] = {'Status': 'Running', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['id']]['EnteredCurrentStatus'], \
                                        'StatusReason': -1}
            else:
                #Job is in some strange state
                trackDict[job['id']] = {'Status': 'Unknown', 'StatusTime': self.classAds[job['id']]['ServerTime'] \
                                        - self.classAds[job['id']]['EnteredCurrentStatus'], \
                                        'StatusReason': self.classAds[job['id']]['JobStatus'] }


        return trackDict









