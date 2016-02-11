#!/usr/bin/env python
"""
_RFCP2Impl_

Implementation of StageOutImpl interface for RFIO in Castor-2
"""
from __future__ import print_function

import os
import re

from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.Storage.Execute import runCommand


class RFCPRALImpl(StageOutImpl):
    """
    _RFCPRALImpl_

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

        targetdir= os.path.dirname(self.parseCastorPath(targetPFN))

        checkdircmd="rfstat %s > /dev/null " % targetdir
        print("Check dir existence : %s" %checkdircmd)
        try:
            checkdirexitCode = self.run(checkdircmd)
        except Exception as ex:
            msg = "Warning: Exception while invoking command:\n"
            msg += "%s\n" % checkdircmd
            msg += "Exception: %s\n" % str(ex)
            msg += "Go on anyway..."
            print(msg)
            pass

        if checkdirexitCode:
            mkdircmd = "rfmkdir -m 775 -p %s" % targetdir
            print("=> creating the dir : %s" %mkdircmd)
            try:
                self.run(mkdircmd)
            except Exception as ex:
                msg = "Warning: Exception while invoking command:\n"
                msg += "%s\n" % mkdircmd
                msg += "Exception: %s\n" % str(ex)
                msg += "Go on anyway..."
                print(msg)
                pass
        else:
            print("=> dir already exists... do nothing.")


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None, checksums = None):
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

    def parseCastorPath(self, complexCastorPath):
        """
        _parseCastorPath_

        Extracts the useful parts of a CASTOR HSM path
        for use in deletion
        """
        simpleCastorPath = None

        if simpleCastorPath == None:
            regExpParser = re.compile('/+castor/ads.rl.ac.uk/(.*)')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/ads.rl.ac.uk/' + match.group(1)

        if simpleCastorPath == None:
            regExpParser = re.compile('rfio:.*/+castor/ads.rl.ac.uk/([^?]+).*')
            match = regExpParser.match(complexCastorPath)
            if ( match != None ):
                simpleCastorPath = '/castor/ads.rl.ac.uk/' + match.group(1)

        # if that does not work just use as-is
        if simpleCastorPath == None:
            simpleCastorPath = complexCastorPath

        # remove multi-slashes from path
        while ( simpleCastorPath.find('//') > -1 ):
            simpleCastorPath = simpleCastorPath.replace('//','/')

        return simpleCastorPath

    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided: specific for Castor-1

        """
        # Extract the CASTOR filepath
        fileToDelete = self.parseCastorPath(pfnToRemove)

        # Does file need removing from cmsTemp, or cmsFarmRead?
        command = ''
        if(re.match('/+castor/ads.rl.ac.uk/prod/cms/store/unmerged/.*', fileToDelete)):
            command = "stager_rm -S cmsTemp -M %s ; nsrm %s" % (fileToDelete, fileToDelete)
        else:
            command = "stager_rm -M %s ; nsrm %s" %(fileToDelete, fileToDelete)
        self.executeCommand(command)

registerStageOutImpl("rfcp-RAL", RFCPRALImpl)
