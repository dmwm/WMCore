#!/usr/bin/env python
"""
_StageOutImpl_

Interface for Stage Out Plugins. All stage out implementations should
inherit this object and implement the methods accordingly

"""
import time
import os
from WMCore.Storage.Execute import runCommand
from WMCore.Storage.StageOutError import StageOutError, StageOutInvalidPath

class StageOutImpl:
    """
    _StageOutImpl_

    Define the interface that needs to be implemented by stage out
    plugins

    Object attributes:

    - *numRetries* : Number of automated retry attempts if the command fails
                     default is 3 attempts
    - *retryPause* : Time in seconds to wait between retries.
                     default is 10 minutes
    """

    def __init__(self, stagein=False):
        self.numRetries = 3
        self.retryPause = 600
        self.stageIn = stagein
        # tuple of exit codes of copy when dest directory does not exist
        self.directoryErrorCodes = tuple()
    

    def deferDirectoryCreation(self):
        """
        Can we defer directory creation, hoping it exists, 
        only to create on a given error condition
        """
        return len(self.directoryErrorCodes) != 0
    

    def executeCommand(self, command):
        """
        _execute_
    
        Execute the command provided, throw a StageOutError if it exits
        non zero
    
        """
        try:
            exitCode = runCommand(command)
            msg = "Command :\n%s\n exited with status: %s" % (command, exitCode)
            print msg
        except Exception, ex:
            msg = "Exception while invoking command:\n"
            msg += "%s\n" % command
            msg += "Exception: %s\n" % str(ex)
            print "ERROR: Exception During Stage Out:\n"
            print msg
            raise StageOutError(msg, Command = command, ExitCode = 60311)
        if exitCode in self.directoryErrorCodes:
            raise StageOutInvalidPath()
        elif exitCode:
            msg = "Command exited non-zero"
            print "ERROR: Exception During Stage Out:\n"
            print msg
            raise StageOutError(msg, Command = command, ExitCode = exitCode)
        return


    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        construct a source URL/PFN for the pfn provided based on the
        protocol that can be passed to the stage command that this
        implementation uses.

        """
        raise NotImplementedError, "StageOutImpl.createSourceName"


    def createTargetName(self, protocol, pfn):
        """
        _createTargetName_

        construct a target URL/PFN for the pfn provided based on the
        protocol that can be passed to the stage command that this
        implementation uses.

        By default this is the same as createSourceName (in cases
        of stage ins the 'local' file is the target). Override this
        in your implementation of this is not the case.

        """
        return self.createSourceName(protocol, pfn)


    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        If a seperate step is required to create a directory in the
        SE for the stage out PFN provided, do that in this command.

        If no directory is required, do not implement this method
        """
        pass


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build a shell command that will transfer the sourcePFN to the
        targetPFN using the options provided if necessary
        
        """
        raise NotImplementedError, "StageOutImpl.createStageOutCommand"


    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        Construct and issue the command to remove the PFN provided as
        this impl requires.
        This will be used by the cleanup nodes in merge jobs that remove the
        intermediate files upon successful completion of the merge job

        """
        raise NotImplementedError, "StageOutImpl.removeFile"


    def createRemoveFileCommand(self, pfn):
        """
        return the command to delete a file after a failed copy
        """
        if pfn.startswith("/"):
            return "/bin/rm -f %s" % pfn
        else:
            return ""


    def __call__(self, protocol, inputPFN, targetPFN, options = None):
        """
        _Operator()_

        This operator does the actual stage out by invoking the overridden
        plugin methods of the derived object.


        """
        #  //
        # // Generate the source PFN from the plain PFN if needed
        #//
        sourcePFN = self.createSourceName(protocol, inputPFN)

        # destination may also need PFN changed
        # i.e. if we are staging in a file from an SE
        targetPFN = self.createTargetName(protocol, targetPFN)

        #  //
        # // Create the output directory if implemented
        #//
        for retryCount in range(1, self.numRetries + 1):
            try:
                # if we can detect directory problems later
                # defer directory creation till then, only applies to stageOut
                if not self.deferDirectoryCreation() or self.stageIn:
                    self.createOutputDirectory(targetPFN)
                break

            except StageOutError, ex:
                msg = "Attempted directory creation for stageout %s failed\n" % retryCount
                msg += "Automatically retrying in %s secs\n " % self.retryPause
                msg += "Error details:\n%s\n" % str(ex)
                print msg
                if retryCount == self.numRetries :
                    #  //
                    # // last retry, propagate exception
                    #//
                    raise ex
                time.sleep(self.retryPause)

        #  //
        # // Create the command to be used.
        #//
        command = self.createStageOutCommand(
            sourcePFN, targetPFN, options)

        #  //
        # // Run the command
        #//
        for retryCount in range(1, self.numRetries + 1):
            try:
                
                try:
                    self.executeCommand(command)
                except StageOutInvalidPath, ex:
                    # plugin indicated directory missing,create and retry
                    msg = "Copy failure indicates directory does not exist.\n"
                    msg += "Create now"
                    print msg
                    self.createOutputDirectory(targetPFN)
                    self.executeCommand(command)
                return

            except StageOutError, ex:
                msg = "Attempted stage out %s failed\n" % retryCount
                msg += "Automatically retrying in %s secs\n " % self.retryPause
                msg += "Error details:\n%s\n" % str(ex)
                print msg
                if retryCount == self.numRetries :
                    #  //
                    # // last retry, propagate exception
                    #//
                    raise ex
                time.sleep(self.retryPause)

        # should never reach this point
        return
