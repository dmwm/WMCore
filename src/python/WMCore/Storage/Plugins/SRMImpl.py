#!/usr/bin/env python
"""
_SRMImpl_

Implementation of StageOutImpl interface for SRM

"""
import os
import logging
import tempfile
from subprocess import Popen, PIPE

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure


_CheckExitCodeOption = True



class SRMImpl(StageOutImplV2):
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



    def doTransfer(self, fromPfn, toPfn, stageOut, seName, command, options, protocol  ):
        toPfn   = self.createSourceName(protocol, toPfn)
        fromPfn = self.createSourceName(protocol, fromPfn)       
        (_,reportFile) = tempfile.mkstemp()
        ourCommand = \
            self.generateCommandFromPreAndPostParts(\
                        ['srmcp','-report=%s'%reportFile,
                                      '-retry_num=0'],
                        [fromPfn, toPfn],
                        options)
        self.runCommandWarnOnNonZero(ourCommand)

        if not self.stageOut:
            remotePFN, localPFN = fromPfn, toPfn.replace("file://", "", 1)
        else:
            remotePFN, localPFN = toPfn, fromPfn.replace("file://", "", 1)
            
        targetPnfsPath = self.createPnfsPath(remotePFN)

        if _CheckExitCodeOption:        
            p1 = Popen(["rfstat", remotePFN], stdout=PIPE)
            p3 = Popen(['cut','-f3','-d" "'], stdin=p1.stdout, stdout=PIPE)
            exitCode = p3.communicate()[0]
            if exitCode:
                raise StageOutError, "srmcp failed! Error code: %s" % exitCode
            
        localSize = os.path.getsize( localPFN )
        logging.info("Local Size %s" % localSize)
        #         filesize() { `srm-get-metadata -retry_num=0 %s 2>/dev/null | grep 'size :[0-9]' | cut -f2 -d":"`}
        # the following replaces the above
        p1 = Popen(["srm-get-metadata", '-retry_num=0', remotePFN], stdout=PIPE)
        p2 = Popen(["grep", "size :[0-9]"], stdout=PIPE, stdin=p1.stdout)
        p3 = Popen(["sed", "-f2",'-d":"'], stdout=PIPE,stdin=p2.stdout)
        remoteSize = p3.communicate()[0]
        logging.info("Localsize: %s Remotesize: %s" % (localSize, remoteSize))
        if int(localSize) != int(remoteSize):
            try:
                self.doDelete(toPfn,None,None,None,None)
            except:
                pass
            raise StageOutFailure, "File sizes don't match"
        
        return toPfn

    
    def doDelete(self, pfn, seName, command, options, protocol  ):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            self.runCommandWarnOnNonZero(["srm-advisory-delete", pfn])
        elif pfn.startswith("file:"):
            self.runCommandWarnOnNonZero(["/bin/rm", "-f", pfn.replace("file://", "", 1)])
        elif pfn.startswith('/'):
            self.runCommandWarnOnNonZero(["/bin/rm", "-f", pfn])
        else:
            logging.info("Tried to delete, but nothing knew how")
            logging.info("pfn: %s" % pfn)
    




