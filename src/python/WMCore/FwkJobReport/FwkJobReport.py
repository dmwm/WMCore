#!/usr/bin/env python
"""
_FwkJobReport_

Toplevel object for representing a Framework Job Report and
manipulating the bits and pieces of it.


"""

from WMCore.FwkJobReport.FileInfo import FileInfo, AnalysisFile
from WMCore.FwkJobReport.PerformanceReport import PerformanceReport
import WMCore.FwkJobReport.StorageStats as StorageStats

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery


class FwkJobReport:
    """
    _FwkJobReport_

    Framework Job Report container and interface object

    """
    def __init__(self, name = None, jobSpecId = None):
        self.name = name
        self.status = None
        self.jobSpecId = jobSpecId
        self.jobType = None
        self.workflowSpecId = None
        self.files = []
        self.inputFiles = []
        self.errors = []
        self.skippedEvents = []
        self.skippedFiles = []
        self.psetHash = None

        #  Set inital exitCode to an error code, it will be updated with
        #    the correct value if the post job steps run correctly
        self.exitCode = 50117
        self.siteDetails = {}
        self.timing = {}
        self.storageStatistics = None
        self.generatorInfo = {}
        self.dashboardId = None
        self.performance = PerformanceReport()
        self.removedFiles = {}
        self.logFiles = {}
        self.unremovedFiles = {}
        self.analysisFiles = []


    def wasSuccess(self):
        """
        _wasSuccess_

        Generate a boolean expression from this report to indicate if
        it comes from a successful job or not.

        This method will return True if:

        exitCode == 0 AND status = "Success"

        Otherwise it will return false

        """
        return (self.exitCode == 0) and (self.status == "Success")


    def sortFiles(self):
        """
        _sortFiles_

        Return a list of output files for this job ordered based on parentage dependencies

        """
        from WMCore.Algorithms.TreeSort import TreeSort
        name = lambda x: x['LFN']
        parents = lambda x: x.parentLFNs()
        return TreeSort(name, parents, self.files).sort()


    def newFile(self):
        """
        _newFile_

        Insert a new file into the Framework Job Report object.
        returns a FwkJobRep.FileInfo
        object by reference that can be populated with extra details of
        the file.

        """
        fileInfo = FileInfo()
        self.isInput = False
        self.files.append(fileInfo)
        return fileInfo

    def newInputFile(self):
        """
        _newInputFile_

        Insert an new Input File into this job report and return the
        corresponding FileInfo instance so that it can be populated

        """
        fileInfo = FileInfo()
        fileInfo.isInput = True
        self.inputFiles.append(fileInfo)
        return fileInfo

    def newAnalysisFile(self):
        """
        _newAnalysisFile_

        Add a description for a new analysis file (non EDM file)
        to this report. Returns the dictionary to be populated

        """
        analysisFile = AnalysisFile()
        self.analysisFiles.append(analysisFile)
        return analysisFile

    def addSkippedEvent(self, runNumber, eventNumber):
        """
        _addSkippedEvent_

        Add a skipped event record run/event number pair
        """
        self.skippedEvents.append(
            {"Run" : runNumber, "Event" : eventNumber}
            )

        return

    def addSkippedFile(self, pfn, lfn):
        """
        _addSkippedFile_

        Add a skipped file record to this report
        """
        self.skippedFiles.append(
            { "Pfn" : pfn, "Lfn" : lfn}
            )
        return


    def addError(self, status, errType):
        """
        _addError_

        Add a new Error dictionary to this report, return it to be populated

        """
        newError = {"ExitStatus" : status,
                    "Type" : errType,
                    "Description": ""}
        self.errors.append(newError)
        return newError


    def addRemovedFile(self, lfn, seName):
        """
        _addRemovedFile_

        Add a record of a removed file, providing the LFN
        and the SEName where the file was removed

        """

        self.removedFiles[lfn] = seName
        return

    def addUnremovedFile (self, lfn, seName):
       """
       _addUnRemovedFile_
       """

       self.unremovedFiles[lfn] = seName

       return

    def addLogFile(self, lfn, seName):
        """
        addLogFile
        """
        self.logFiles[lfn] = seName
        return


    def save(self):
        """
        _save_

        Save the Framework Job Report by converting it into
        an XML IMProv Object

        """
        result = IMProvNode("FrameworkJobReport")
        if self.name != None:
            result.attrs['Name'] = self.name
        if self.status != None:
            result.attrs['Status'] = str(self.status)
        if self.jobSpecId != None:
            result.attrs['JobSpecID'] = self.jobSpecId
        if self.workflowSpecId != None:
            result.attrs['WorkflowSpecID'] = self.workflowSpecId
        if self.jobType != None:
            result.attrs['JobType'] = self.jobType
        if self.dashboardId != None:
            result.attrs['DashboardId'] = self.dashboardId

        #  //
        # // Save ExitCode
        #//
        result.addNode(
            IMProvNode("ExitCode",
                       None,
                       Value = str(self.exitCode)
                       )
            )

        #  //
        # // Save Site details
        #//
        for key, value in self.siteDetails.items():
            siteDetail = IMProvNode("SiteDetail", None,
                                    Parameter = key,
                                    Value = str(value))

            result.addNode(siteDetail)
        #  //
        # // Save PSetHash
        #//
        if self.psetHash != None:
            result.addNode(IMProvNode("PSetHash", str(self.psetHash)))

        #  //
        # // Save Files
        #//
        for fileInfo in self.files:
            result.addNode(fileInfo.save())

        #  //
        # // Save Input Files
        #//
        for infileInfo in self.inputFiles:
            result.addNode(infileInfo.save())

        #  //
        # // Save Analysis Files
        #//
        for aFileInfo in self.analysisFiles:
            result.addNode(aFileInfo.save())
        #  //
        # // Save Skipped Events
        #//
        for skipped in self.skippedEvents:
            result.addNode(IMProvNode("SkippedEvent", None,
                                      Run = skipped['Run'],
                                      Event = skipped['Event']))
        #  //
        # // Save Skipped Files
        #//
        for skipped in self.skippedFiles:
            result.addNode(IMProvNode("SkippedFile", None,
                                      Pfn = skipped['Pfn'],
                                      Lfn = skipped['Lfn']))

        #  //
        # // Save Removed Files
        #//
        for remLfn, remSE in self.removedFiles.items():
            result.addNode(IMProvNode("RemovedFile", remLfn, SEName=remSE))

        #  //
        # // Save Unremoved Files
        #//
        for unremLfn, unremSE in self.unremovedFiles.items():

            result.addNode(IMProvNode("UnremovedFile", unremLfn, SEName=unremSE))


        #  //
        # // Save Log Files
        #//
        for logLfn, logSE in self.logFiles.items():
            result.addNode(IMProvNode("LogFile", logLfn, SEName=logSE))

        #  //
        # // Save Errors
        #//
        for error in self.errors:
            result.addNode(
                IMProvNode("FrameworkError", error['Description'],
                           ExitStatus = error['ExitStatus'],
                           Type = error['Type'])
                )

        #  //
        # // Save Timing Info
        #//
        timing = IMProvNode("TimingService")
        result.addNode(timing)
        for key, value in self.timing.items():
            timing.addNode(IMProvNode(key, None, Value=str(value) ))

        #  //
        # // Save Storage Statistics
        #//
        if self.storageStatistics != None:
            result.addNode(
                IMProvNode("StorageStatistics", self.storageStatistics))

        genInfo = IMProvNode("GeneratorInfo")
        result.addNode(genInfo)
        for key, val in self.generatorInfo.items():
            genInfo.addNode(IMProvNode("Data", None, Name = key,
                                       Value = str(val)))


        #  //
        # // Save Performance Report
        #//
        result.addNode(self.performance.save())

        return result

    def write(self, filename):
        """
        _write_

        Write the job report to an XML file

        """
        handle = open(filename, 'w')
        handle.write(self.save().makeDOMElement().toprettyxml())
        handle.close()
        return


    def __str__(self):
        """strin representation of instance"""
        return str(self.save())


    def load(self, improvNode):
        """
        _load_

        Unpack improvNode into this instance

        """
        self.name = improvNode.attrs.get("Name", None)
        self.status = improvNode.attrs.get("Status", None)
        self.jobSpecId = improvNode.attrs.get("JobSpecID", None)
        self.jobType = improvNode.attrs.get("JobType", None)
        self.workflowSpecId =improvNode.attrs.get("WorkflowSpecID", None)
        self.dashboardId =improvNode.attrs.get("DashboardId", None)

        exitQ = IMProvQuery(
            "/FrameworkJobReport/ExitCode[attribute(\"Value\")]")
        exitVals = exitQ(improvNode)
        if len(exitVals) > 0:
            self.exitCode = int(exitVals[-1])

        #  //
        # // Site details
        #//
        siteQ = IMProvQuery("/FrameworkJobReport/SiteDetail")
        [ self.siteDetails.__setitem__(x.attrs['Parameter'], x.attrs['Value'])
          for x in siteQ(improvNode) ]

        #  //
        # // PSetHash
        #//
        hashQ = IMProvQuery("/FrameworkJobReport/PSetHash[text()]")
        hashVals = hashQ(improvNode)
        if len(hashVals) > 0:
            hashData = hashVals[-1]
            self.psetHash = str(hashData)


        #  //
        # // output files
        #//
        fileQ = IMProvQuery("/FrameworkJobReport/File")
        fileList = fileQ(improvNode)
        for fileEntry in fileList:
            newFile = self.newFile()
            newFile.load(fileEntry)

        #  //
        # // input files
        #//
        infileQ = IMProvQuery("/FrameworkJobReport/InputFile")
        for infileEntry in infileQ(improvNode):
            newInFile = self.newInputFile()
            newInFile.load(infileEntry)

        #  //
        # // Skipped Events & Files
        #//
        skipFileQ = IMProvQuery("/FrameworkJobReport/SkippedFile")
        skipEventQ = IMProvQuery("/FrameworkJobReport/SkippedEvent")


        [ self.addSkippedEvent(int(skipEv.attrs['Run']),
                               int(skipEv.attrs['Event']))
          for skipEv in skipEventQ(improvNode)]

        [ self.addSkippedFile(skipF.attrs['Pfn'], skipF.attrs['Lfn'])
          for skipF in skipFileQ(improvNode) ]

        #  //
        # // removed files
        #//
        remFileQ = IMProvQuery("/FrameworkJobReport/RemovedFile")
        [ self.addRemovedFile(str(remF.chardata), remF.attrs['SEName'])
          for remF in remFileQ(improvNode) ]

        #  //
        # // unremoved files
        #//
        unremFileQ = IMProvQuery("/FrameworkJobReport/UnremovedFile")
        [ self.addUnremovedFile(str(remF.chardata), remF.attrs['SEName'])
          for remF in unremFileQ(improvNode) ]

        #  //
        # // analysis files
        #//
        afileQ = IMProvQuery("/FrameworkJobReport/AnalysisFile")
        for afileEntry in afileQ(improvNode):
            newAFile = self.newAnalysisFile()
            newAFile.load(afileEntry)
        #  //
        # // log files
        #//
        logFileQ = IMProvQuery("/FrameworkJobReport/LogFile")
        [ self.addLogFile(str(logF.chardata), logF.attrs['SEName'])
          for logF in logFileQ(improvNode) ]

        #  //
        # // Timing, Storage and generator info
        #//
        timingQ = IMProvQuery("/FrameworkJobReport/TimingService/*")

        [ self.timing.__setitem__(x.name, x.attrs['Value'])
          for x in timingQ(improvNode) ]

        storageQ = IMProvQuery("/FrameworkJobReport/StorageStatistics")
        storageInfo = storageQ(improvNode)
        if len(storageInfo) > 0:
            #self.storageStatistics = storageInfo[-1]
            StorageStats.handleStorageStats(storageInfo[-1], self)

        genQ = IMProvQuery("/FrameworkJobReport/GeneratorInfo/Data")
        [ self.generatorInfo.__setitem__(x.attrs['Name'], x.attrs['Value'])
          for x in genQ(improvNode)]

        errQ = IMProvQuery("/FrameworkJobReport/FrameworkError")
        errors  = errQ(improvNode)
        for err in errors:
            newErr = self.addError(
                int(err.attrs['ExitStatus']),
                err.attrs['Type'])
            newErr['Description'] = err.chardata

        #  //
        # // Performance reports
        #//
        self.performance.load(improvNode)

        return

    def __to_json__(self, thunker):
        """
        __to_json__

        Pull all the meta data out of this and stuff it into a dict.  Pass any
        complex objects to the thunker so that they can be serialized.
        """
        fwkjrDict = {"name": self.name, "status": self.status,
                     "jobSpecId": self.jobSpecId, "jobType": self.jobType,
                     "workflowSpecId": self.workflowSpecId,
                     "errors": self.errors, "skippedEvents": self.skippedEvents,
                     "skippedFiles": self.skippedFiles,
                     "psetHash": self.psetHash, "exitCode": self.exitCode,
                     "siteDetails": self.siteDetails, "timing": self.timing,
                     "generatorInfo": self.generatorInfo,
                     "dashboardId": self.dashboardId,
                     "removedFiles": self.removedFiles,
                     "unremovedFiles": self.unremovedFiles,
                     "logFiles": self.logFiles}

        fwkjrDict["files"] = []
        for file in self.files:
            fwkjrDict["files"].append(thunker._thunk(file))

        fwkjrDict["input_files"] = []
        for file in self.inputFiles:
            fwkjrDict["input_files"].append(thunker._thunk(file))            

        fwkjrDict["performance"] = thunker._thunk(self.performance)
        return fwkjrDict
