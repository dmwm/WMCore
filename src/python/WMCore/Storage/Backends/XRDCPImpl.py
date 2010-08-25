#!/usr/bin/env python
"""
_XRDCPImpl_

Implementation of StageOutImpl interface for RFIO in Castor-2

"""
import os 
from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl

from WMCore.Storage.Execute import runCommand


class XRDCPImpl(StageOutImpl):
    """
    _XRDCPImpl_

    Implement interface for rfcp command
    
    """

    run = staticmethod(runCommand)

    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

         uses pfn

        """
        return "%s" % pfn

    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        """
        _createStageOutCommand_

        Build an xrdcp command

        """
        original_size = os.stat(sourcePFN)[6]
        print "Local File Size is: %s" % original_size
        result = ". /afs/cern.ch/user/c/cmsprod/scratch1/releases/CMSSW_1_8_0_pre0/src/runtime.sh ; xrdcp -d 3"
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " root://lxgate39.cern.ch/%s " % targetPFN
        result += "; DEST_SIZE=`rfstat %s | grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && [ '%s' == $DEST_SIZE ]; then exit 0; else echo \"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (targetPFN,original_size)
        return result

    
    def removeFile(self, pfnToRemove):
        """
        _removeFile_

        CleanUp pfn provided: specific for Castor-2

        """
        command = "stager_rm -M %s ; nsrm %s" %(pfnToRemove,pfnToRemove)
        self.executeCommand(command)


registerStageOutImpl("xrdcp", XRDCPImpl)
