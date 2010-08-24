#!/usr/bin/env python
"""
_SRMImpl_

Implementation of StageOutImpl interface for SRM

"""
import os
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl


_CheckExitCodeOption = True



class SRMImpl(StageOutImpl):
    """
    _SRMImpl_

    Implement interface for srmcp command
    
    """
    
    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if pfn.startswith('/'):
            return "file:///%s" % pfn
        else:
            return pfn

    def createPnfsPath(self,pfn) :
        """
        _createPnfsPath_

        convert SRM pfn to PNDS pfn

        """
        return '/pnfs/cms/WAX' + pfn.split('=')[1]

    def createRemoveFileCommand(self, pfn):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            return "/bin/rm -f %s" % self.createPnfsPath(pfn)
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
        result += "srmcp -report=$REPORT_FILE -retry_num=0 "
        
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s \n" % targetPFN

#        # generate target pnfs path
#        # remap source and dest depending on which is local and which remote
#        for path in (sourcePFN, targetPFN):
#            if path.startswith('srm://'):
#                #targetPFN = path
#                targetPnfsPath = self.createPnfsPath(path)
#            else:
#                #sourcePFN = path
#                pass

        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN.replace("file://", "", 1)
        else:
            remotePFN, localPFN = targetPFN, sourcePFN.replace("file://", "", 1)
            
        targetPnfsPath = self.createPnfsPath(remotePFN)
#        for filePath in (sourcePFN, targetPFN):
#            if filePath.startswith("srm://"):
#                remotePFN = filePath
#                targetPnfsPath = self.createPnfsPath(filePath)
#                localPFN = filePath.replace("file://", "", 1)
#            else:
#                # assume this is the local file
#                localPFN = filePath.replace("file://", "", 1)

        if _CheckExitCodeOption:
            result += """
            EXIT_STATUS=`cat $REPORT_FILE | cut -f3 -d" "`
            echo "srmcp exit status: $EXIT_STATUS"

            if [[ $EXIT_STATUS != 0 ]]; then
               echo "Non-zero srmcp Exit status!!!"
               echo "Cleaning up failed file:"
               %s
               exit 60311
            fi
        
            """ % self.createRemoveFileCommand(targetPnfsPath)
        
        result += "FILE_SIZE=`stat -c %s"
        result += " %s `\n" % localPFN
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"
        metadataCheck = \
        """
        filesize() { cat "`dirname $1`/.(use)(2)(`basename $1`)'" | grep l= | sed -e's/.*;l=\([0-9]*\).*/\\1/'; }

        SRM_SIZE=`filesize %s`
        echo "SRM Size is $SRM_SIZE"
        if [[ $SRM_SIZE > 0 ]]; then
           if [[ $SRM_SIZE == $FILE_SIZE ]]; then
              exit 0
           else
              echo "Error: Size Mismatch between local and SE"
              echo "Cleaning up failed file:"
              /bin/rm -f %s
              exit 60311
           fi 
        fi
        echo "Cleaning up failed file:"
        /bin/rm -f %s 
        exit 60311

        """ % ( targetPnfsPath, targetPnfsPath, targetPnfsPath)
        result += metadataCheck
        
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "/bin/rm -f %s" % self.createPnfsPath(pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("srm-fnal", SRMImpl)
