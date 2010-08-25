#!/usr/bin/env python
"""
_CPImpl_

Implementation of StageOutImpl interface for plain cp

"""
import os 
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.Storage.Execute import runCommand


class CPImpl(StageOutImpl):
    """
    _CPImpl_

    Implement interface for plain cp command
    
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

        checkdircmd="/bin/ls %s > /dev/null " % targetdir
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
           mkdircmd = "/bin/mkdir -m 775 -p %s" % targetdir
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

        Build an cp command

        """
        original_size = os.stat(sourcePFN)[6]
        print "Local File Size is: %s" % original_size
        result = "/bin/cp "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        result += "; DEST_SIZE=`/bin/ls -l %s | /bin/awk '{print $5}'` ; if [ $DEST_SIZE ] && [ '%s' == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (targetPFN,original_size)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "/bin/rm %s" % pfnToRemove
        self.executeCommand(command)


registerStageOutImpl("cp", CPImpl)
