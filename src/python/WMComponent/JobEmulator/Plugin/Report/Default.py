#!/usr/bin/env python
"""
_Default_

Plugin for the Job Emulator to generate job reports
upon job completion.  Successful reports are marked
as success while failure are marked a middleware
failures.
"""

from random import randrange
from random import random
from random import gauss
from random import choice

import logging
import traceback

from WMCore.FwkJobReport.FJR import FJR

#FIXME: needs to reference to wmcore library
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
#FIXME: needs to reference to wmcore library
from ProdCommon.FwkJobRep.RunInfo import RunInfo
#FIXME: needs to reference to wmcore library
from ProdCommon.MCPayloads.DatasetTools import getOutputDatasetDetails
#FIXME: needs to reference to wmcore library
from ProdCommon.MCPayloads.MergeTools import getSizeBasedMergeDatasetsFromNode
#FIXME: needs to reference to wmcore library
from ProdCommon.MCPayloads.UUID import makeUUID


class Default:

    def successReport(self, jobSpecLoaded, workerNodeInfo, reportFilePath):
        """
        _successReport_

        Create a job report representing the successful completion
        of a job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        jobSpecPayload, newReport = \
                self.__fwkJobReportCommon(jobSpecLoaded, workerNodeInfo)
        newReport.exitCode = 0
        newReport.status = "Success"

        if "jobId" in jobSpecLoaded.parameters.keys():
            newReport.jobSpecId = jobSpecLoaded.parameters["jobId"]

        # Create a list of datasets from the JobSpec
        # then associate file to these later on
        datasets = getOutputDatasetDetails(jobSpecPayload)
        datasets.extend(getSizeBasedMergeDatasetsFromNode(jobSpecPayload))
        if jobSpecPayload.cfgInterface == None:
            outModules = {}
        else:
            outModules = jobSpecPayload.cfgInterface.outputModules

        if jobSpecPayload.cfgInterface == None:
            inputFiles = []
        else:
            inputFiles = jobSpecPayload.cfgInterface.inputFiles

        for dataset in datasets:
            modName = dataset.get('OutputModuleName', None)

            if outModules.has_key(modName):
                dataset['LFNBase'] = outModules[modName].get('LFNBase', None)
                self.setDefaultForNoneValue('LFNBase', dataset['LFNBase'])
                dataset['MergedLFNBase'] = \
                                outModules[modName].get('MergedLFNBase', None)

        datasetMap = {}
        for dataset in datasets:
            datasetMap[dataset['OutputModuleName']] = dataset


        for outName, outMod in \
            outModules.items():

            theFile = newReport.newFile()
            guid = makeUUID()

            if outMod.has_key("LFNBase"):
                theFile['LFN'] = "%s/%s.root" % (outMod['LFNBase'], guid)
            else:
                theFile['LFN'] = "/some/madeup/path/%s.root" % guid

            self.setDefaultForNoneValue('LFNBase', theFile['LFN'])
            theFile['PFN'] ="fakefile:%s" % theFile['LFN']
            theFile['GUID'] = guid
            theFile['MergedBySize'] = choice(["True", "False"])
            theFile['ModuleLabel'] = outName
            # basic measurement is byte (minumum 4MB, max 4GB)
            theFile['Size'] = 4000000 * randrange(1, 1000)
            runNum = jobSpecLoaded.parameters["RunNumber"]
            # need to get lumi
            lumiList = jobSpecLoaded.parameters.get("LumiSections", [])
            theFile.runs[runNum] = RunInfo(runNum, *lumiList)
            #check if the maxEvents['output'] is set if not set totalEvent using maxEvents['input']
            totalEvent = jobSpecPayload.cfgInterface.maxEvents['output']
            if totalEvent == None:
                totalEvent = jobSpecPayload.cfgInterface.maxEvents['input']

            # if there is no input and output, print out error message and set default to 1000
            totalEvent = self.setDefaultForNoneValue(
                                           "maxEvent['input' and 'output']",
                                            totalEvent,
                                            100)

            try:
                totalEvent = int(totalEvent)
            except ValueError, ex:
                logging.error("totalEvent is not a number. \n%s" % ex)

            # event size should be  >= 0
            # totalEvent is  -1 process all event
            if totalEvent < 0:
                totalEvent = 200

            if (random() > self.avgEventProcessingRate):
                # Gauss distribution of totalEvent.
                meanEvent = int(totalEvent * 0.7)
                stdDev = totalEvent * 0.15
                tempTotalEvent = int(gauss(meanEvent,stdDev))
                if tempTotalEvent <= 0 :
                    totalEvent = 1
                elif tempTotalEvent >= totalEvent:
                    totalEvent = totalEvent - 1
                else:
                    totalEvent = tempTotalEvent

            #logging.debug("---------- Total Event ----------: %s \n" % totalEvent)
            theFile['TotalEvents'] = totalEvent

            theFile['SEName'] = workerNodeInfo['se-name']
            theFile['CEname'] = workerNodeInfo['ce-name']
            theFile['Catalog'] = outMod['catalog']
            theFile['OutputModuleClass'] = "PoolOutputModule"

            theFile.addChecksum("cksum", randrange(1000000, 10000000))
            theFile.branches.extend(["fakeBranch_%d-%s.Rec" % (num, guid)
                                  for num in range(randrange(5,20))])
            #theFile.load(theFile.save())
            theFile["BranchHash"] = randrange(2000000, 30000000)
            [ theFile.addInputFile("fakefile:%s" % x , "%s" % x )
              for x in inputFiles ]

            if datasetMap.has_key(outName):
                datasetForFile = theFile.newDataset()
                datasetForFile.update(datasetMap[outName])


        newReport.write(reportFilePath)

        return newReport

    def failureReport(self, jobSpecLoaded, workerNodeInfo, reportFilePath):
        """
        _failureReport_

        Create a job report representing the failure of a job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        print('START: Implement me: failureReport')
        print('END: Implement me: failureReport')


    def __fwkJobReportCommon(self, jobSpecLoaded, workerNodeInfo):
        """
        __fwkJobReportCommon_

        Create a new job report and fill it in with generic
        information that is not dependent on the outcome of
        the job.

        The jobSpecLoaded parameter is a reference to an instance
        of the JobSpec class that has been initialized with the
        job spec that we are generating a report for.

        """
        try:
            jobSpecPayload = jobSpecLoaded.payload

            newReport = FJR()
            newReport.jobSpecId = jobSpecPayload.jobName
            newReport.jobType = jobSpecPayload.type
            newReport.workflowSpecId = jobSpecPayload.workflow
            newReport.name = jobSpecPayload.name
            #get information from the super class
            parts = workerNodeInfo[1].split('_')
            siteName = 'FAKE_'+ parts[1]+'_'+parts[2]
            newReport.siteDetails['SiteName'] = siteName
            #HostName is the same as worker_node name
            newReport.siteDetails['HostName'] = workerNodeInfo[0]
            newReport.siteDetails['se-name'] = 'se@'+siteName
            newReport.siteDetails['ce-name'] = 'ce@'+siteName
            newReport.addLogFile("/path/to/log/archive", "some.random.se.somewhere")

            if not hasattr(jobSpecPayload, "cfgInterface"):
                jobSpecPayload.cfgInterface = None

            return jobSpecPayload, newReport

        except Exception, ex:
            #msg = "Unable to Publish Report for %s\n" % jobSpecPayload.jobName
            #msg += "Since It is not known to the JobState System:\n"
            msg = str(ex)
            logging.error(traceback.format_exc())
            logging.error(msg)

            raise RuntimeError, msg

