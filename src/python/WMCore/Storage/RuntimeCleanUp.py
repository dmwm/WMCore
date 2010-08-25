#!/usr/bin/env python
"""
_RuntimeCleanUp_

Runtime binary file for CleanUp type nodes

"""
import sys
import os
from WMCore.Storage.TaskState import TaskState, getTaskState
import StageOut.Impl
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.FwkJobReport import FwkJobReport
from WMCore.Storage.MergeReports import mergeReports



class CleanUpSuccess(Exception):
    """
    _CleanUpSuccess_

    """
    def __init__(self, lfn, pfn):
        Exception.__init__(self, "CleanUpSuccess")
        msg = "Succesful Cleanup of LFN:\n%s\n" % lfn
        msg += "  PFN: %s\n" % pfn
        print msg

class CleanUpFailure(Exception):
    """
    _CleanUpFailure_

    """
    def __init__(self, lfn, **details):
        Exception.__init__(self, "CleanUpFailure")
        self.lfn = lfn
        self.details = details
        msg = "================CleanUp Failure========================\n"
        msg += " Failed to clean up file:\n"
        msg += " %s\n" % lfn
        msg += " Details:\n"
        for key, val in details.items():
            msg += "  %s: %s\n" % (key, val)

        print msg
        
class SkippedFileFilter:
    def __init__(self, skippedFiles):
        self.skipped = [ i['Lfn'] for i in skippedFiles ] 

    def __call__(self, filedata):
        return filedata['LFN'] not in self.skipped

class CleanUpManager:
    """
    _CleanUpManager_

    Object that is invoked to do the cleanup operation

    """
    def __init__(self, cleanUpTaskState, inputTaskState = None ):
        self.state = cleanUpTaskState
        self.inputState = inputTaskState
        self.jobFail = False 
        #  //
        # // load templates
        #//
        self.taskName = self.state.taskAttrs['Name']
        self.config = self.state._RunResDB.toDictionary()[self.taskName]

        if self.inputState != None:
            self.cleanUpInput()
        else:
            self.cleanUpFileList()

        self.setupCleanup()
        

    def cleanUpInput(self):
        """
        _cleanUpInput_

        This cleanup node is for cleaning after an input job
        Eg post merge cleanup
        
        """
        msg = "Cleaning up input files for job: "
        msg += self.inputState.taskAttrs['Name']
        print msg
        self.inputState.loadJobReport()
        inputReport = self.inputState.getJobReport()
        
        inputFileDetails = filter(
            SkippedFileFilter(inputReport.skippedFiles),
            inputReport.inputFiles)

        
        self.inputFiles = [ i['LFN'] for i in inputFileDetails ] 


    def cleanUpFileList(self):
        """
        _cleanUpFileList_

        List of LFNs is provided in the RunResDB for this node

        """
        lfnList = self.config.get("RemoveLFN", [])

        if len(lfnList) == 0:
          
          # //
          # // Retriving list of lfn's from Jobspec
          # //	  
          self.state.loadJobSpecNode()                   
          lfnList = self.state.jobSpecNode.configuration.split() 
          
        msg = "Cleaning up list of files:\n"

                  
        if len(lfnList) == 0:
            msg += "No Files Found in Configuration!!!"

        for lfn in lfnList:
            msg += " Removing: %s\n" % lfn

        print msg

        self.inputFiles = lfnList
        return
        
        
        

    def setupCleanup(self):
        """
        _setupCleanup_

        Setup for cleanup operation: Read in siteconf and TFC

        """
        
        self.success = []
        self.failed = []
        
        #  //
        # // Try an get the TFC for the site
        #//
        self.tfc = None
        siteCfg = self.state.getSiteConfig()
        self.seName =  siteCfg.localStageOutSEName() 
            
        if siteCfg == None:
            msg = "No Site Config Available:\n"
            msg += "Unable to perform CleanUp operation"
            raise RuntimeError, msg
        
        try:
            self.tfc = siteCfg.trivialFileCatalog()
            msg = "Trivial File Catalog has been loaded:\n"
            msg += str(self.tfc)
            print msg
        except StandardError, ex:
            msg = "Unable to load Trivial File Catalog:\n"
            msg += "Clean Up will not be attempted\n"
            msg += str(ex)
            raise RuntimeError, msg

        
        
        #  //
        # // Lookup StageOut Impl name that will be used to generate
        #//  cleanup
        self.implName = siteCfg.localStageOut.get("command", None)
        if self.implName == None:
            msg = "Unable to retrieve local stage out command\n"
            msg += "From site config file.\n"
            msg += "Unable to perform CleanUp operation"
            raise RuntimeError, msg
        msg = "Stage Out Implementation to be used for cleanup is:"
        msg += "%s" % self.implName
        print msg
        
        

    def __call__(self):
        """
        _operator()_

        Invoke cleanup operation

        """
        for deleteFile in self.inputFiles:
           
            try:
                print "Deleting File: %s" % deleteFile
                self.invokeCleanUp(deleteFile)
                self.success.append(deleteFile)
            except CleanUpFailure, ex:
                
                   self.failed.append(deleteFile)

                   if not (ex.details.has_key('TFC')):

                      self.jobFail = True
               


                

        status = 0
        msg = "The following LFNs have been cleaned up successfully:\n"
        for lfn in self.success:
            msg += "  %s\n" % lfn
        
        if self.jobFail is True:
            
            status = 60312

        msg = "Exit Status for this task is: %s\n" % status
        print msg
        
	# //
	# // Writing framework Jobreport for cleanup jobs
	# //
	
        self.processCleanUpJobReport(status)
	
        return status

        
    def invokeCleanUp(self, lfn):
        """
        _invokeCleanUp_

        Instantiate the StageOut impl, map the LFN to PFN using the TFC
        and invoke the CleanUp on that PFN

        """
        #  //
        # // Load Impl
        #//
        try:
            
            implInstance = retrieveStageOutImpl(self.implName)
        except Exception, ex:
            msg = "Error retrieving Stage Out Impl for name: "
            msg += "%s\n" % self.implName
            msg += str(ex)
            raise CleanUpFailure(lfn, 
                                 ImplName = self.implName,
                                 Message = msg)
        
        #  //
        # // Match LFN
        #//
        pfn = self.tfc.matchLFN(self.tfc.preferredProtocol, lfn)
        if pfn == None:
            msg = "Unable to map LFN to PFN:\n"
            msg += "LFN: %s\n" % lfn
            raise CleanUpFailure(lfn, TFC = str(self.tfc),
                                 ImplName = self.implName,
                                 Message = msg,
                                 TFCProtocol = self.tfc.preferredProtocol)
        
        #  //
        # //  Invoke StageOut Impl removeFile method
        #//
        try:
                      
            implInstance.removeFile(pfn)
        except Exception, ex:
            msg = "Error performing Cleanup command for impl "
            msg += "%s\n" % self.implName
            msg += "On PFN: %s\n" % pfn
            msg += str(ex)
               
            # //
            # // Will uncomment it after invalidating deleted lfn from mergesensordb
            # //

            raise CleanUpFailure(lfn, TFC = str(self.tfc),
                                 ImplName = self.implName,
                                 PFN = pfn,
                                 Message = msg,
                                 TFCProtocol = self.tfc.preferredProtocol)
        





    def processCleanUpJobReport(self,statusCode):
        """
        _processCleanUpJobReport_

        Arguments:
             None
        Return:
             None

        """


       
        
        
        #  //
        # //  Generate a report
        #  //
        report = FwkJobReport()
        report.name = "cleanUp"
	report.status = "Failed" 
	
	if statusCode == 0 :
          report.status = "Success"
          for lfnRemovedFile in self.success:
            report.addRemovedFile(lfnRemovedFile, self.seName)    
        
        
        for lfnUnremovedFile in self.failed:
             report.addUnremovedFile(lfnUnremovedFile, self.seName)
             	
 
        report.exitCode = statusCode
        report.jobSpecId = self.state.jobSpecNode.jobName
        report.jobType = self.state.jobSpecNode.jobType
        report.workflowSpecId = self.state.jobSpecNode.workflow
        
        report.write("./FrameworkJobReport.xml")

        #  //
        # // Ensure this report gets added to the job-wide report
        #//
        toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],"FrameworkJobReport.xml")
        newReport = os.path.join(os.getcwd(), "FrameworkJobReport.xml")
        mergeReports(toplevelReport, newReport)
    
    
        


def cleanUp():
    """
    _cleanUp_

    Main program

    """
       
    state = TaskState(os.getcwd())
    state.loadRunResDB()
        
    try:
        
        config = state._RunResDB.toDictionary()[state.taskAttrs['Name']]
         
    except StandardError, ex:
        msg = "Unable to load details from task directory:\n"
        msg += "Error reading RunResDB XML file:\n"
        msg += "%s\n" % state.runresdb 
        msg += "and extracting details for task in: %s\n" % os.getcwd()
        print msg
        exitCode = 60312
        f = open("exit.status", 'w')
        f.write(str(exitCode))
        f.close()
        sys.exit(exitCode)

    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState
    cleanUpParam = config.get('CleanUpParameters',{})
    cleanUpFor = cleanUpParam.get('CleanUpFor',None)
    inputState = None
    if cleanUpFor != None:
      inputTask = config['CleanUpParameters']['CleanUpFor'][0]
      inputState = getTaskState(inputTask)        
    
    manager = CleanUpManager(state, inputState)
    exitCode = manager()
     

    return exitCode










if __name__ == '__main__':
    print "RuntimeCleanUp invoked..."
    exitCode = cleanUp()
    
    f = open("exit.status", 'w')
    f.write(str(exitCode))
    f.close()
   
    sys.exit(exitCode)
     
