#!/usr/bin/env python
"""
_LCGImpl_

Implementation of StageOutImpl interface for lcg-cp

"""
import os, re
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True



class LCGImpl(StageOutImpl):
    """
    _LCGImpl_

    Implement interface for srmcp v2 command with lcg-* commands
    
    """
    
    run = staticmethod(runCommand)
    
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if pfn.startswith('/'):
            return "file:%s" % pfn
        else:
            return pfn
        

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_
        
        lcg-cp handles this

        """
        return
                 
    def createRemoveFileCommand(self, pfn):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            return "lcg-del -b -l -D srmv2 --vo cms %s" % pfn
        elif pfn.startswith("file:"):
            return "/bin/rm -f %s" % pfn.replace("file:", "", 1)
        else:
            return StageOutImpl.createRemoveFileCommand(self, pfn)
        

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an srmcp command

        """
        result = "#!/bin/sh\n"
        result += "lcg-cp -b -D srmv2 --vo cms -t 2400 --verbose "
        
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s \n" % targetPFN
        
        
        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=$?
            echo "lcg-cp exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
               echo "Non-zero lcg-cp Exit status!!!"
               echo "Cleaning up failed file:"
                %s 
               exit 60311
            fi
        
            """ % self.createRemoveFileCommand(targetPFN)
        
        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN.replace("file:", "", 1)
        else:
            remotePFN, localPFN = targetPFN, sourcePFN.replace("file:", "", 1)
        
        result += "FILE_SIZE=`stat -c %s"
        result += " %s `\n" % localPFN
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"
        
        metadataCheck = \
        """
        for ((a=1; a <= 10 ; a++))
        do
           SRM_SIZE=`lcg-ls -l -b -D srmv2 %s 2>/dev/null | awk '{print $5}'`
           echo "Remote Size is $SRM_SIZE"
           if [[ $SRM_SIZE > 0 ]]; then
              if [[ $SRM_SIZE == $FILE_SIZE ]]; then
                 exit 0
              else
                 echo "Error: Size Mismatch between local and SE"
                 echo "Cleaning up failed file:"
                 %s 
                 exit 60311
              fi 
           else
              sleep 2
           fi
        done
        echo "Cleaning up failed file:"
        %s 
        exit 60311

        """ % (remotePFN, self.createRemoveFileCommand(targetPFN), self.createRemoveFileCommand(targetPFN))
        result += metadataCheck
        
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "lcg-del -b -l -D srmv2 --vo cms %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("srmv2-lcg", LCGImpl)
