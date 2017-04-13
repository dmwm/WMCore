#!/usr/bin/env python
"""
_LuminosityBased_

Luminosity based splitting algorithm that will adapt the number of events in a job according to already known
event processing time, information which is collected by the monitoring systems. Complete discussion at :

https://hypernews.cern.ch/HyperNews/CMS/get/wmDevelopment/499.html
"""

import re
import os
import httplib
import json
import logging
import traceback
import urllib2
from math import ceil

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

class LuminosityBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        An event base splitting algorithm.  All available files are split into a
        set number of events per job.
        """
        
        eventsPerJob = int(kwargs.get("events_per_job", 100))
        eventsPerLumi = int(kwargs.get("events_per_lumi", eventsPerJob))
        getParents   = kwargs.get("include_parents", False)
        lheInput = kwargs.get("lheInputFiles", False)
        collectionName  = kwargs.get('collectionName', None)
        primaryDataset  = kwargs.get('primaryDataset', None)
        cmsswversion  = kwargs.get('cmsswversion', None)
        targetJobLength = int(kwargs.get('targetJobLength', 21600))
        testDqmLuminosityPerLs = kwargs.get('testDqmLuminosityPerLs', None)
        testPerfCurve = kwargs.get('testPerfCurve', None)
        minLuminosity = int(kwargs.get('minLuminosity', 1))
        maxLuminosity = int(kwargs.get('maxLuminosity', 9000))
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))
        acdcFileList = []

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL       = kwargs.get('couchURL')
                couchDB        = kwargs.get('couchDB')
                filesetName    = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                owner          = kwargs.get('owner')
                group          = kwargs.get('group')
                logging.info('Creating jobs for ACDC fileset %s' % filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                acdcFileList = dcs.getProductionACDCInfo(collectionName, filesetName, owner, group)
            except Exception, ex:
                msg =  "Exception while trying to load goodRunList\n"
                msg +=  "Refusing to create any jobs.\n"
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                return

        totalJobs    = 0

        locationDict = self.sortByLocation()
        perfCurveCache = {}
        dqmLuminosityPerLsCache = {}
        for location in locationDict:
            self.newGroup()
            fileList = locationDict[location]

            if self.package == 'WMCore.WMBS':
                loadRunLumi = self.daoFactory(
                                    classname = "Files.GetBulkRunLumi")
                fileLumis = loadRunLumi.execute(files = fileList)
                for f in fileList:
                    lumiDict = fileLumis.get(f['id'], {})
                    for run in lumiDict.keys():
                        f.addRun(run = Run(run, *lumiDict[run]))

            for f in fileList:
                currentEvent = f['first_event']
                eventsInFile = f['events']
                # Keeping this one just in case, but we know that we will have 1 run per file
                runs = list(f['runs'])
                for runObject in runs :
                    run = runObject.run
        
                    # If we have it beforehand is because the test sent it from the test file.
                    if not testDqmLuminosityPerLs:
                        # Test if the curve is in the Cache before fecthing it from DQM
                        if not dqmLuminosityPerLsCache.has_key(run):
                            dqmLuminosityPerLs = self.getLuminosityPerLsFromDQM(run)
                            dqmLuminosityPerLsCache[run] = dqmLuminosityPerLs
                    else :
                        dqmLuminosityPerLs = testDqmLuminosityPerLs

                    if not testPerfCurve: # If we have it forehand is because the test sent it from the test file.
                        # Test if the curve is in the Cache before fecthing it from DQM
                        if not perfCurveCache.has_key(cmsswversion+primaryDataset): 
                            perfCurve = self.getPerfCurve(cmsswversion, primaryDataset)
                            perfCurveCache[cmsswversion+primaryDataset] = perfCurve
                        #perfCurve = self.getPerfCurve(cmsswversion, primaryDataset)
                    else :
                        perfCurve = testPerfCurve

                    # Now we got everything :
                    #  * Lumi-section range of file
                    #  * All sorts of information (luminosity, perf)
                    # So we should do the following :
                    #  * Get avg luminosity of the file range
                    fileTimePerEvent = 0 
                    if dqmLuminosityPerLs :
                        avgLumi = self.getFileAvgLuminosity(f, dqmLuminosityPerLs)

                        #  * Get closest point in the curve, if multiple, average. Acceptable range should be defined
                        # Interesting feature here : if it finds too much points with the given precision (3rd param)
                        # It will call itself again, lowering the precision by 0.1 steps until it finds less than 5, more than 2 points
                        # This way we can have a much more precise range into the curve, if there is a lot of data
                        fileTimePerEvent = self.getFileTimePerEvent(avgLumi, perfCurve, 0.10)
                        #  * If this can't be found, (not enough data somewhere, use timePerEvent)
                    if fileTimePerEvent == 0 :
                        fileTimePerEvent = timePerEvent
                    #  * Get the TpE and find how much eventsPerJob we want for this file.
                    eventsPerJob = int(targetJobLength/fileTimePerEvent)
                    if fileTimePerEvent != timePerEvent :
                        logging.debug("This file has average instantaneous luminosity %f average time per event %f and is getting %i events per job" % 
                                        (avgLumi, fileTimePerEvent, eventsPerJob))
                    else : 
                        logging.debug("This file did not get enough performance information and is getting manual TpE %f , therefore %i events per job" %
                                        (fileTimePerEvent, eventsPerJob))

                    # Ask about this in the list.
                    if not f['lfn'].startswith("MCFakeFile"):
                        #Then we know for sure it is not a MCFakeFile, so process
                        #it as usual
                        if eventsInFile >= eventsPerJob:
                            while currentEvent < eventsInFile:
                                self.newJob(name = self.getJobName(length=totalJobs))
                                self.currentJob.addFile(f)
                                if eventsPerJob + currentEvent < eventsInFile:
                                    jobTime = eventsPerJob * timePerEvent
                                    diskRequired = eventsPerJob * sizePerEvent
                                    self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                                else:
                                    jobTime = (eventsInFile - currentEvent) * timePerEvent
                                    diskRequired = (eventsInFile - currentEvent) * sizePerEvent
                                    self.currentJob["mask"].setMaxAndSkipEvents(eventsInFile,
                                                                                currentEvent)
                                self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                                     memory = memoryRequirement,
                                                                     disk = diskRequired)
                                currentEvent += eventsPerJob
                                totalJobs    += 1
                        else:
                            self.newJob(name = self.getJobName(length=totalJobs))
                            self.currentJob.addFile(f)
                            jobTime = eventsInFile * timePerEvent
                            diskRequired = eventsInFile * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                                 memory = memoryRequirement,
                                                                 disk = diskRequired)
                            totalJobs += 1
                    else:
                    	# DO we need to worry about ACDC? Likely, but then the IF condition should be based in the ACDCFILELIST variable and not random FakeMC file
                        if acdcFileList:
                            if f['lfn'] in [x['lfn'] for x in acdcFileList]:
                                totalJobs = self.createACDCJobs(f, acdcFileList,
                                                                timePerEvent, sizePerEvent, memoryRequirement,
                                                                lheInput, totalJobs)
                            continue

    def createACDCJobs(self, fakeFile, acdcFileInfo,
                       timePerEvent, sizePerEvent, memoryRequirement,
                       lheInputOption, totalJobs = 0):
        """
        _createACDCJobs_

        Create ACDC production jobs, this are treated differentely
        since it is an exact copy of the failed jobs.
        """
        for acdcFile in acdcFileInfo:
            if fakeFile['lfn'] == acdcFile['lfn']:
                self.newJob(name = self.getJobName(length = totalJobs))
                self.currentJob.addBaggageParameter("lheInputFiles", lheInputOption)
                self.currentJob.addFile(fakeFile)
                self.currentJob["mask"].setMaxAndSkipEvents(acdcFile["events"],
                                                            acdcFile["first_event"])
                self.currentJob["mask"].setMaxAndSkipLumis(len(acdcFile["lumis"]) - 1,
                                                           acdcFile["lumis"][0])
                jobTime = (acdcFile["events"] - acdcFile["first_event"] + 1) * timePerEvent
                diskRequired = (acdcFile["events"] - acdcFile["first_event"] + 1) * sizePerEvent
                self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                     memory = memoryRequirement,
                                                     disk = diskRequired)
                totalJobs += 1
        return totalJobs

    def getFileAvgLuminosity(self, f, dqmLuminosityPerLs):
        runs = list(f['runs'])
        lumis = runs[0].lumis

        totalLumi = 0 
        for lumiSection in lumis :
            totalLumi += dqmLuminosityPerLs[lumiSection]
        avgLumi = totalLumi/float(len(lumis))
        # Attention here, people being eager to do quick math led to several mistakes in the past.
        # The luminosity we get from DQM is INTEGRATED and in nano barns, or 10e33, while all
        # monitoring bases itself in INSTANTANEOUS luminosity, usually in 10e30 scales.
        # We will then have to divide avgLumi by 23, length of a LS in the LHC Run1.
        avgLumi = (avgLumi*1000)/23
        
        return avgLumi
            
    def getFileTimePerEvent(self, avgLumi, perfCurve, precision = 0.1, enoughPrecision = False):
        # This is a very interesting part of the algorithm, where it adapts the precision according
        # to the number of points obtained, if too much it raises the precision so we get more narrow
        # in the selection of points, close to the average that we want. It also has some smart handling
        # of situations where it lowers the precision and gets nothing, then it goes one step back.

        # Find points in a range of 5% of the avgLumi
        stdDev = int(avgLumi*precision)
        logging.debug("Searching for performance information, luminosity %f, precision : %f" % (avgLumi, precision))

        interestingPoints = list()
        for point in perfCurve :
            if point[0] > avgLumi-stdDev and point[0] < avgLumi+stdDev :
                interestingPoints.append(point)                

        logging.debug("found %i interesting points" % len(interestingPoints))

        if len(interestingPoints) == 0 :
            if precision != 0.1 :
                self.getFileTimePerEvent(avgLumi, perfCurve, precision = precision+0.01, enoughPrecision = True)
            return 0

        # If we have too much data, call itself again, with more precision
        if len(interestingPoints) > 5 and precision > 0.01 and enoughPrecision == False:
            self.getFileTimePerEvent(avgLumi, perfCurve, precision = precision-0.01)

        # Finally calculates the average time per event and returns it
        totalSec = 0
        for point in interestingPoints:
            totalSec += point[1]
        avgTPE = totalSec/len(interestingPoints)
           
        return avgTPE

    def getLuminosityPerLsFromDQM(self, run):

        if os.getenv("X509_USER_PROXY") is None:
            return False
                    
        # Get the proxy, as CMSWEB doesn't allow us to use plain HTTP
        hostCert = os.getenv("X509_USER_PROXY")    
        hostKey  = hostCert
        try :
            os.stat(hostCert)        
        except OSError :
            logging.debug("X509_USER_PROXY was not really there, can't query DQM GUI")
            return False
        dqmUrl = "https://cmsweb.cern.ch/dqm/online/"
        getUrl = "%sjsonfairy/archive/%s/Global/Online/ALL/PixelLumi/PixelLumiDqmZeroBias/totalPixelLumiByLS" % (dqmUrl, str(run))
        
        regExp=re.compile('https://(.*)(/dqm.+)')
        regExpResult = regExp.match(getUrl)
        dqmHost = regExpResult.group(1)
        dqmPath = regExpResult.group(2)
        
        connection = httplib.HTTPSConnection(dqmHost, 443, hostKey, hostCert)
        connection.request('GET', dqmPath)
        response = connection.getresponse()
        responseData = response.read()
        responseJSON = json.loads(responseData)
        if not responseJSON["hist"]["bins"].has_key("content") :
            logging.info("Actually got a JSON from DQM perf in for run %d  , but content was bad, Bailing out"
                         % run)
            return False
        logging.debug("We have the DQM performance curve")
        return responseJSON

    def getPerfCurve(self, cmsswversion, primaryDataset):
    
        dashbUrl = "http://dashb-luminosity.cern.ch/dashboard/request.py/getpoints?luminosityFrom=1&luminosityTo=9000&release=%s&primaryDataset=%s" % (cmsswversion, primaryDataset)

        try:
            response = urllib2.urlopen(dashbUrl)

        except urllib2.HTTPError, e:
            logging.debug("HTTP Error when fetching performance curve : %s" % e.code)
            return "error"
        except urllib2.URLError, e:
            logging.debug("URLError in perfCurve fetching. Check the agent's connectivity")
            return "error"

        dashbJson = json.loads(response.read())

    
        return dashbJson["points"][0]["data"]
    
