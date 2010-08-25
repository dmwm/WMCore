#!/usr/bin/env python
"""
_SRMV2Impl_

Implementation of StageOutImpl interface for SRM Version 2

"""
import os, re
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutError

from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True



class SRMV2Impl(StageOutImpl):
    """
    _SRMV2Impl_

    Implement interface for srmcp v2 command
    
    """
    
    run = staticmethod(runCommand)
    
    def __init__(self, stagein=False):
        StageOutImpl.__init__(self, stagein)
        self.directoryErrorCodes = (1,)
    
    
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if pfn.startswith('/'):
            return "file:///%s" % pfn
        else:
            return pfn
        

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_
        
        SRMV2 does not create directories, 
            see http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html#_Toc199734394

        """
        targetdir = os.path.dirname(targetPFN)
        
        if self.stageIn:
            # stage in to local directory - should exist but you never know
            if not os.path.exists(targetdir):
                os.makedirs(targetdir)
            return
        
        mkdircommand = "srmmkdir -retry_num=%s " % self.numRetries
        checkdircmd="srmls -recursion_depth=0 -retry_num=%s " % self.numRetries
        
        #  // Loop from top level checking existence stop when directory exists
        # // assume first 4 slashes are from srm://host:8443/srm/managerv2?SFN=
        dirs = ["/".join(targetdir.split("/")[0:6+i]) \
                                        for i in range(targetdir.count("/")-4)]
        dirsToCheck = dirs[:]; dirsToCheck.reverse()
        levelToCreateFrom = len(dirs)
        for count, dir in zip(range(len(dirsToCheck), 0, -1), dirsToCheck):
            try:
                exitCode, output = self.run(checkdircmd + dir)
                levelToCreateFrom = count # create dirs from here (at least)
                if exitCode: # did srmls fail to execute properly?
                    raise RuntimeError, "Error checking directory existence, %s" % str(output)
                if not output.count('SRM_FAILURE'): # any other codes?
                    break
            except Exception, ex:
                 msg = "Warning: Exception while invoking command:\n"
                 msg += "%s\n" % checkdircmd + dir
                 msg += "Exception: %s\n" % str(ex)
                 msg += "Go on anyway..."
                 print msg
                 pass

        #  // Create needed directory levels from end of previous loop
        # //  to end of directory structure
        for dir in dirs[levelToCreateFrom:]:
            print "Create directory: %s" % dir
            try:
                exitCode, output = self.run(mkdircommand + dir)
                if exitCode:
                    raise RuntimeError, "Error creating directory, %s" % str(output)
            except Exception, ex:
                 msg = "Warning: Exception while invoking command:\n"
                 msg += "%s\n" % mkdircommand + dir
                 msg += "Exception: %s\n" % str(ex)
                 msg += "Go on anyway..."
                 print msg
                 pass
                 
    def createRemoveFileCommand(self, pfn):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            return "srmrm %s" % pfn
        elif pfn.startswith("file:"):
            return "/bin/rm -f %s" % pfn.replace("file://", "", 1)
        else:
            return StageOutImpl.createRemoveFileCommand(self, pfn)
        

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an srmcp command

        """
        result = "#!/bin/sh\n"
        result += "REPORT_FILE=`pwd`/srm.report.$$\n"
        result += "srmcp -2 -report=$REPORT_FILE -retry_num=0"
        
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s" % targetPFN
        result += " 2>&1 | tee srm.output.$$ \n"
        
        
        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=`cat $REPORT_FILE | cut -f3 -d" "`
            echo "srmcp exit status: $EXIT_STATUS"
            if [[ "X$EXIT_STATUS" == "X" ]] && [[ `grep -c SRM_INVALID_PATH srm.output.$$` != 0 ]]; then
                exit 1 # dir does not eixst
            elif [[ $EXIT_STATUS != 0 ]]; then
               echo "Non-zero srmcp Exit status!!!"
               echo "Cleaning up failed file:"
                %s 
               exit 60311
            fi
        
            """ % self.createRemoveFileCommand(targetPFN)
        
        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN.replace("file://", "", 1)
        else:
            remotePFN, localPFN = targetPFN, sourcePFN.replace("file://", "", 1)
        
        #targetPFN =  remotePFN
        remotePath = None
        SFN = '?SFN='
        sfn_idx = remotePFN.find('?SFN=')
        if sfn_idx >= 0:
            remotePath = remotePFN[sfn_idx+5:]
        r = re.compile('srm://([A-Za-z\-\.0-9]*)(:[0-9]*)?(/.*)')
        m = r.match(remotePFN)
        if not m:
            raise StageOutError("Unable to determine path from PFN for " \
                                "target %s." % filePath)
        if remotePath == None:
            remotePath = m.groups()[2]
        remoteHost = m.groups()[0]
                   
#        for filePath in (sourcePFN, targetPFN):
#            if filePath.startswith("file://"):
#                localPFN = filePath.replace("file://", "")
#            elif filePath.startswith("srm://"):
#                remotePFN = filePath
#                targetPFN = filePath
#                targetPath = None
#                SFN = '?SFN='
#                sfn_idx = filePath.find(SFN)
#                if sfn_idx >= 0:
#                    targetPath = filePath[sfn_idx+5:]
#                r = re.compile('srm://([A-Za-z\-\.0-9]*)(:[0-9]*)?(/.*)')
#                m = r.match(filePath)
#                if not m:
#                    raise StageOutError("Unable to determine path from PFN for " \
#                                "target %s." % filePath)
#                if targetPath == None:
#                    targetPath = m.groups()[2]
#                targetHost = m.groups()[0]
        
        result += "FILE_SIZE=`stat -c %s"
        result += " %s `\n" % localPFN
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"
        
        metadataCheck = \
        """
        for ((a=1; a <= 10 ; a++))
        do
           SRM_SIZE=`srmls -recursion_depth=0 -retry_num=0 %s 2>/dev/null | grep '%s' | grep -v '%s' | awk '{print $1;}'`
           echo "SRM Size is $SRM_SIZE"
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

        """ % (remotePFN, remotePath, remoteHost, self.createRemoveFileCommand(targetPFN), self.createRemoveFileCommand(targetPFN))
        result += metadataCheck
        
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "srmrm %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("srmv2", SRMV2Impl)
