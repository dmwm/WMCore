from __future__ import (print_function, division)

import unittest

from mock import mock

from WMCore.Storage.Backends.SRMImpl import SRMImpl


class SRMImplTest(unittest.TestCase):
    def setUp(self):
        self.SRMImpl = SRMImpl()

    def testCreateSourceName_simple(self):
        self.assertEqual("name", self.SRMImpl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.SRMImpl.createSourceName("", "/name"))

    @mock.patch('WMCore.Storage.Backends.SRMImpl.os.path')
    def testCreateSourceName_withPath(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "test"
        self.assertEqual("file:///test", self.SRMImpl.createSourceName("", "name"))

    def testCreateRemoveFileCommand(self):
        self.assertEqual("srm-advisory-delete -2 -retry_num=0 srm://test",
                         self.SRMImpl.createRemoveFileCommand("srm://test"))
        self.assertEqual("/bin/rm -f test", self.SRMImpl.createRemoveFileCommand("file://test"))
        self.assertEqual("/bin/rm -f file:test", self.SRMImpl.createRemoveFileCommand("file:test"))

    def testCreateRemoveFileCommand_stageOut(self):
        self.assertEqual("/bin/rm -f /test", self.SRMImpl.createRemoveFileCommand("/test"))
        self.assertEqual("", self.SRMImpl.createRemoveFileCommand("test"))

    @mock.patch('WMCore.Storage.Backends.SRMImpl.os.path')
    def testCreateRemoveFileCommand_stageOut_withPath(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "test"
        self.assertEqual("/bin/rm -f test", self.SRMImpl.createRemoveFileCommand("ftest"))

    @mock.patch('WMCore.Storage.Backends.SRMImpl.SRMImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_options(self, mock_createRemoveFileCommand):
        self.SRMImpl.stageIn = True
        mock_createRemoveFileCommand.return_value = "test"
        sourcePFN = remotePFN = "sourcePFN"
        targetPFN = localPFN = "targetPFN"
        result = self.SRMImpl.createStageOutCommand(sourcePFN, targetPFN, options="testOptions")
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN,
                                                                 options="testOptions")
        self.assertEqual(expectedResults, result)

    @mock.patch('WMCore.Storage.Backends.SRMImpl.SRMImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_stageInFalse(self, mock_createRemoveFileCommand):
        self.SRMImpl.stageIn = False
        mock_createRemoveFileCommand.return_value = "test"
        sourcePFN = "file://sourcePFN"
        targetPFN = "file://targetPFNfile://"
        remotePFN = "file://targetPFNfile://"
        localPFN = "sourcePFN"
        result = self.SRMImpl.createStageOutCommand(sourcePFN, targetPFN)
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN)
        self.assertEqual(expectedResults, result)

    @mock.patch('WMCore.Storage.Backends.SRMImpl.SRMImpl.createRemoveFileCommand')
    def testCreateStageOutCommand_stageInTrue(self, mock_createRemoveFileCommand):
        self.SRMImpl.stageIn = True
        mock_createRemoveFileCommand.return_value = "test"
        sourcePFN = "file://sourcePFNfile://"
        targetPFN = "file://targetPFNfile://"
        remotePFN = "file://sourcePFNfile://"
        localPFN = "targetPFNfile://"
        result = self.SRMImpl.createStageOutCommand(sourcePFN, targetPFN)
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN)
        self.assertEqual(expectedResults, result)

    def getResultForCreateStageOutCommand(self, sourcePFN, targetPFN, localPFN, remotePFN,
                                          createRemoveFileCommandValue="test", options=None):
        result = "#!/bin/sh\n"
        result += "REPORT_FILE=`pwd`/srm.report.$$\n"
        result += "srmcp -report=$REPORT_FILE -retry_num=0 -request_lifetime=2400 "
        if options:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s \n" % targetPFN
        result += """
            EXIT_STATUS=`cat $REPORT_FILE | cut -f3 -d" "`
            echo "srmcp exit status: $EXIT_STATUS"
            if [[ $EXIT_STATUS != 0 ]]; then
               echo "Non-zero srmcp Exit status!!!"
               echo "Cleaning up failed file:"
               %s
               exit 60311
            fi

            """ % createRemoveFileCommandValue
        result += "FILE_SIZE=`stat -c %s"
        result += " %s `\n" % localPFN
        result += "echo \"Local File Size is: $FILE_SIZE\"\n"
        metadataCheck = \
        """
        SRM_SIZE=`srm-get-metadata -retry_num=0 %s 2>/dev/null | grep 'size :[0-9]' | cut -f2 -d":"`
        echo "SRM Size is $SRM_SIZE"
        if [[ $SRM_SIZE == $FILE_SIZE ]]; then
           exit 0
        else
           echo "Error: Size Mismatch between local and SE"
           echo "Cleaning up failed file:"
           %s
           exit 60311
        fi
        fi
        """ % (remotePFN, createRemoveFileCommandValue)
        result += metadataCheck
        return result

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.SRMImpl.removeFile("file")
        mock_executeCommand.assert_called_with("srm-advisory-delete -2 -retry_num=0 file")
