#!/usr/bin/env python
"""
_SRMV2Impl_

Implementation of StageOutImpl interface for SRM Version 2

"""
from builtins import zip, range

import os, re
import logging, tempfile
import subprocess
from subprocess import Popen, PIPE

from WMCore.Storage.StageOutImplV2 import StageOutImplV2
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure

from WMCore.Storage.Execute import runCommandWithOutput as runCommand

_CheckExitCodeOption = True



class SRMV2Impl(StageOutImplV2):
    """
    _SRMV2Impl_

    Implement interface for srmcp v2 command

    """

    run = staticmethod(runCommand)

    def __init__(self):
        StageOutImplV2.__init__(self)
        self.directoryErrorCodes = (1,)


    def createSourceName(self, protocol, pfn):
        """
        _createSourceName_

        SRM uses file:/// urls

        """
        if pfn.startswith('/'):
            return "file:///%s" % pfn
        elif os.path.isfile(pfn):
            return "file:///%s" % os.path.abspath(pfn)
        else:
            return pfn


    def createOutputDirectory(self, targetPFN, stageOut):
        """
        _createOutputDirectory_

        SRMV2 does not create directories,
            see http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html#_Toc199734394

        """
        targetdir = os.path.dirname(targetPFN)

        if not stageOut:
            # stage in to local directory - should exist but you never know
            if not os.path.exists(targetdir):
                os.makedirs(targetdir)
            return

        #  // Loop from top level checking existence stop when directory exists
        # // assume first 4 slashes are from srm://host:8443/srm/managerv2?SFN=
        dirs = ["/".join(targetdir.split("/")[0:6+i]) \
                                        for i in range(targetdir.count("/")-4)]
        dirsToCheck = dirs[:]; dirsToCheck.reverse()
        levelToCreateFrom = len(dirs)
        # the -1 in the zip is because we assume that /store is there
        for count, dir in zip(range(len(dirsToCheck), 0, -1), dirsToCheck):
            _, output = self.runCommandWarnOnError(['srmls',\
                                                           '-recursion_depth=0',\
                                                            '-retry_num=%s'\
                                                              % self.numRetries,\
                                                            dir])
            levelToCreateFrom = count # create dirs from here (at least)
            if not output.count('SRM_FAILURE'): # any other codes?
                break

        #  // Create needed directory levels from end of previous loop
        # //  to end of directory structure
        for dir in dirs[levelToCreateFrom - 1:]:
            #print "Create directory: %s" % dir
            self.runCommandWarnOnError(['srmmkdir',
                                        '-retry_num=%s' % self.numRetries,
                                        dir])

    def doDelete(self, pfn, pnn, command, options, protocol  ):
        """
        handle both srm and file pfn types
        """
        if pfn.startswith("srm://"):
            self.runCommandWarnOnNonZero(["srmrm", 'pfn'])
        elif pfn.startswith("file:"):
            self.runCommandWarnOnNonZero(["/bin/rm", "-f", pfn.replace("file://", "", 1)])
        elif pfn.startswith('/'):
            self.runCommandWarnOnNonZero(["/bin/rm", "-f", pfn])
        elif os.path.isfile(pfn):
            self.runCommandWarnOnNonZero(["/bin/rm", "-f", os.path.abspath(pfn)])
        else:
            logging.info("Tried to delete, but nothing knew how")
            logging.info("pfn: %s" % pfn)

    def doTransfer(self, fromPfn, toPfn, stageOut, pnn, command, options, protocol, checksum  ):
        toPfn   = self.createSourceName(protocol, toPfn)
        fromPfn = self.createSourceName(protocol, fromPfn)
        # TODO tee the output to another file
        # attempt copy
        for x in range(self.numRetries):
            (_,reportFile) = tempfile.mkstemp()
            ourCommand = \
                self.generateCommandFromPreAndPostParts(\
                            ['srmcp','-2','-report=%s'%reportFile,
                                          '-retry_num=0'],
                            [fromPfn, toPfn],
                            options)
            self.runCommandWarnOnNonZero(ourCommand)

            if not stageOut:
                remotePFN, localPFN = fromPfn, toPfn.replace("file://", "", 1)
            else:
                remotePFN, localPFN = toPfn, fromPfn.replace("file://", "", 1)


            if _CheckExitCodeOption:
                p1 = Popen(["cat", reportFile], stdout=PIPE)
                p3 = Popen(['cut','-f3','-d',' '], stdin=p1.stdout, stdout=PIPE)
                exitCode = p3.communicate()[0].rstrip()
                logging.info("srmcp exit status: %s" % exitCode)
                p2 = Popen(['grep', '-c', 'SRM_INVALID_PATH',reportFile],stdout=PIPE)
                invalidPathCount = p2.communicate()[0]
                logging.info("got this for SRM_INVALID_PATH: %s" % invalidPathCount)
                if (invalidPathCount and (exitCode == '')):
                    logging.warning("Directory doesn't exist in srmv2 stageout...creating and retrying")
                    self.createOutputDirectory(toPfn,stageOut)
                    continue
                elif ( str(exitCode) != "0" ):
                    logging.error("Couldn't stage out! Error code: %s" % exitCode)
                    self.doDelete(toPfn,None,None,None,None)
                    raise StageOutFailure("srmcp failed! Error code: %s" % exitCode)
                else:
                    logging.info("Tentatively succeeded transfer, will check metadata")
                    break

        localSize = os.path.getsize( localPFN )
        logging.info("Local Size %s" % localSize)

        remotePath = None
        SFN = '?SFN='
        sfn_idx = remotePFN.find(SFN)
        if sfn_idx >= 0:
            remotePath = remotePFN[sfn_idx+5:]
        r = re.compile('srm://([A-Za-z\-\.0-9]*)(:[0-9]*)?(/.*)')
        m = r.match(remotePFN)
        if not m:
            raise StageOutError("Unable to determine path from PFN for " \
                                "target %s." % remotePFN)
        if remotePath == None:
            remotePath = m.groups()[2]
        remoteHost = m.groups()[0]
        #         filesize() { `srm-get-metadata -retry_num=0 %s 2>/dev/null | grep 'size :[0-9]' | cut -f2 -d":"`}
        # the following replaces the above
        logging.info("remote path: %s" % remotePath)
        logging.info("remote host: %s" % remoteHost)
        p1 = Popen(["srmls", '-recursion_depth=0','-retry_num=0', remotePFN], stdout=PIPE)
        p2 = Popen(["grep", remotePath], stdout=PIPE, stdin=p1.stdout)
        p3 = Popen(["grep", '-v', remoteHost], stdout=PIPE, stdin=p2.stdout)
        p4 = Popen(["awk", "{print $1;}"], stdout=PIPE,stdin=p3.stdout)
        remoteSize = p4.communicate()[0]
        logging.info("Localsize: %s Remotesize: %s" % (localSize, remoteSize))
        if int(localSize) != int(remoteSize):
            try:
                self.doDelete(toPfn,None,None,None,None)
            except:
                pass
            raise StageOutFailure("File sizes don't match")

        return toPfn

    def runCommandWarnOnError(self,command):
        return SRMV2Impl.runCommandWarnOnError(self, command)
