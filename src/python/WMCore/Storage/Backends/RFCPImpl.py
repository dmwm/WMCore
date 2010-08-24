#!/usr/bin/env python
"""
_RFCPImpl_

Implementation of StageOutImpl interface for RFIO

"""
import os 
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.Storage.Execute import runCommand


class RFCPImpl(StageOutImpl):
    """
    _RFCPImpl_

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

        targetdir= self.getDirname(targetPFN)

        checkdircmd="rfstat \"%s\" > /dev/null " % targetdir
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
        result += " \"%s\" " % sourcePFN
        result += " \"%s\" " % targetPFN
        
        if self.stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
        
        result += "\nFILE_SIZE=`rfstat \"%s\" | grep Size | cut -f2 -d:`\n" % localPFN
        result += " echo \"Local File Size is: $FILE_SIZE\"; DEST_SIZE=`rfstat \"%s\" | grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && [ $FILE_SIZE == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (remotePFN)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided

        """
        command = "rfrm \"%s\"" % pfnToRemove
        self.executeCommand(command)

    def getDirname(self, pfn ):
        """
        _getDirname_

        Parse directory name out of rfio: PFN

        """

        start=0
        path=""

        if pfn.startswith( "rfio:" ):
            if pfn.find( "path=" ) != -1:
                # first form, everything after path= is the path
                dummy,path = pfn.split("path=")
            else:
                if pfn.find( "?" ) != -1:
                    # second form, path is between the third slash and the ?
                    path,dummy = pfn.split("?")
                else:
                    # illegal form that seems to work rfio:///<path>
                    path = pfn
                start = path.find( "//" ) # find 1st two
                start = path.find( "/", start+2 ) # find 3rd
                if path.find( "/", start+1 ) == start+1:
                    # if there is a 4th next get rid of the third
                    start += 1
                path = path[start:]
        else:
            path = pfn

        return os.path.dirname( path )


registerStageOutImpl("rfcp", RFCPImpl)
