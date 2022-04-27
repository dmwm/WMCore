from __future__ import (print_function, division)

import unittest
import os

from mock import mock
from WMCore.Storage.Backends.XRDCPImpl import XRDCPImpl


class XRDCPImplTest(unittest.TestCase):
    def setUp(self):
        self.XRDCPImpl = XRDCPImpl()

    def testCreateSourceName_simple(self):
        self.assertEqual("name", self.XRDCPImpl.createSourceName("", "name"))

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_noStageInNoOptionsNoChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = False
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResults = self.createStageOutCommandResults(False, "sourcePFN", "targetPFN", "sourcePFN", None, False,
                                                            False, "host", "path")
        self.assertEqual(expectedResults, results)

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_stageInNoOptionsNoChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = True
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResults = self.createStageOutCommandResults(True, "sourcePFN", "targetPFN", "targetPFN", None, False,
                                                            False, "host", "path")
        self.assertEqual(expectedResults, results)

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_optionsNoStageInNoChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = False
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN", options="--wma-cerncastor")
        expectedResults = self.createStageOutCommandResults(False, "sourcePFN", "targetPFN", "sourcePFN",
                                                            "--wma-cerncastor", False, False, "host", "path")
        self.assertEqual(expectedResults, results)

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_optionsUnknowsNoStageInNoChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = False
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN", options="--wma-cerncastor --test")
        expectedResults = self.createStageOutCommandResults(False, "sourcePFN", "targetPFN", "sourcePFN",
                                                            "--wma-cerncastor", "--test", False, "host", "path")
        self.assertEqual(expectedResults, results)

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_optionsUnknowsStageInNoChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = True
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN", options="--wma-cerncastor --test")
        expectedResults = self.createStageOutCommandResults(True, "sourcePFN", "targetPFN", "targetPFN", "--wma-cerncastor",
                                                            "--test", False, "host", "path")
        self.assertEqual(expectedResults, results)

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_optionsUnknowsStageInChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = True
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN", options="--wma-cerncastor --test",
                                                       checksums=["adler32"])
        expectedResults = self.createStageOutCommandResults(True, "sourcePFN", "targetPFN", "targetPFN", "--wma-cerncastor",
                                                            "--test", False, "host", "path")
        self.assertEqual(expectedResults, results)

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.splitPFN')
    def testCreateStageOutCommand_optionsNoStageInChecksum(self, mock_splitPFN):
        mock_splitPFN.return_value = (None, "host", "path", None)
        self.XRDCPImpl.stageIn = False
        results = self.XRDCPImpl.createStageOutCommand("sourcePFN", "targetPFN", options="--wma-cerncastor --test",
                                                       checksums={'adler32': "32"})
        expectedResults = self.createStageOutCommandResults(False, "sourcePFN", "targetPFN", "sourcePFN",
                                                            "--wma-cerncastor", "--test", "00000032", "host", "path")
        self.assertEqual(expectedResults, results)

    def createStageOutCommandResults(self, stageIn, sourcePFN, targetPFN, localPFN, copyCommandOptions, unknow,
                                     checksums, host, path):
        copyCommand = ""
        if not stageIn:
            copyCommand += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
            if copyCommandOptions:
                targetPFN += "?svcClass=t0cms"
        initFile = "/cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/current/el7-x86_64/setup.sh"
        if not self.XRDCPImpl._checkXRDUtilsExist() and os.path.isfile(initFile):
            copyCommand += "source {}\n".format(initFile)
        copyCommand += "xrdcp --force --nopbar "
        if unknow:
            copyCommand += "%s " % unknow
        if checksums:
            copyCommand += "--cksum adler32:%s " % checksums
        copyCommand += " \"%s\" " % sourcePFN
        copyCommand += " \"%s\" \n" % targetPFN
        if stageIn:
            copyCommand += "LOCAL_SIZE=`stat -c%%s \"%s\"`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
            removeCommand = ""
        else:
            removeCommand = "xrdfs '%s' rm %s ;" % (host, path)
        copyCommand += "REMOTE_SIZE=`xrdfs '%s' stat '%s' | grep Size | sed -r 's/.*Size:[ ]*([0-9]+).*/\\1/'`\n" % (
            host, path)
        copyCommand += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"
        if checksums:
            copyCommand += "echo \"Local File Checksum is: %s\"\n" % checksums
            copyCommand += "REMOTE_XS=`xrdfs '%s' query checksum '%s' | grep -i adler32 | sed -r 's/.*[adler|ADLER]32[ ]*([0-9a-fA-F]{8}).*/\\1/'`\n" % (
                host, path)
            copyCommand += "echo \"Remote File Checksum is: $REMOTE_XS\"\n"

            copyCommand += "if [ $REMOTE_SIZE ] && [ $REMOTE_XS ] && [ $LOCAL_SIZE == $REMOTE_SIZE ] && [ '%s' == $REMOTE_XS ]; then exit 0; " % \
                           checksums
            copyCommand += "else echo \"ERROR: Size or Checksum Mismatch between local and SE\"; %s exit 60311 ; fi" % removeCommand
        else:
            copyCommand += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
            copyCommand += "else echo \"ERROR: Size Mismatch between local and SE\"; %s exit 60311 ; fi" % removeCommand
        return copyCommand

    @mock.patch('WMCore.Storage.Backends.XRDCPImpl.XRDCPImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.XRDCPImpl.removeFile("gsiftp://site.com/inputs/f.a")
        mock_executeCommand.assert_called_with("xrdfs site.com rm inputs/f.a")
