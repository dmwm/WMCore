#!/usr/bin/env python
"""
_RFCP2Impl_

Implementation of StageOutImpl interface for RFIO in Castor-2

"""
import os 
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.Storage.Execute import runCommand


class RFCP2Impl(StageOutImpl):
    """
    _RFCP2Impl_

    Implement interface for rfcp command
    
    """

    run = staticmethod(runCommand)

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

         uses pfn

        """
        return "%s" % pfn

    def createOutputDirectory(self, targetPFN):
        """
        _createOutputDirectory_

        create dir with group permission
        """
        
        targetdir= os.path.dirname(targetPFN)

        checkdircmd="rfstat %s > /dev/null " % targetdir
        print "Check dir existence : %s" %checkdircmd 
        try:
          checkdirexitCode = self.run(checkdircmd)
        except Exception, ex:
             msg = "Warning: Exception while invoking command:\n"
             msg += "%s\n" % checkdircmd
             msg += "Exception: %s\n" % str(ex)
             msg += "Go on anyway..."
             print msg
             pass

        if checkdirexitCode:
           mkdircmd = "rfmkdir -m 775 -p %s" % targetdir
           print "=> creating the dir : %s" %mkdircmd
           try:
             self.run(mkdircmd)
           except Exception, ex:
             msg = "Warning: Exception while invoking command:\n"
             msg += "%s\n" % mkdircmd
             msg += "Exception: %s\n" % str(ex)
             msg += "Go on anyway..."
             print msg
             pass
        else:
           print "=> dir already exists... do nothing."


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an rfcp command

        """
        result = "rfcp "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        
        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
        
        result += "\nFILE_SIZE=`stat -c %s"
        result += " %s `;\n" % localPFN
        result += " echo \"Local File Size is: $FILE_SIZE\"; DEST_SIZE=`rfstat %s | grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && [ $FILE_SIZE == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (remotePFN)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided: specific for Castor-1

        """
        command = "stager_rm -M %s ; nsrm %s" %(pfnToRemove,pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("rfcp-2", RFCP2Impl)
