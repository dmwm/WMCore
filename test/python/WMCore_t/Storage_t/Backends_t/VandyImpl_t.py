from __future__ import (print_function, division)

import unittest

from mock import mock

from WMCore.Storage.Backends.VandyImpl import VandyImpl


class VandyImplTest(unittest.TestCase):
    def setUp(self):
        self.VandyImpl = VandyImpl()

    def testCreateSourceName_simple(self):
        self.assertEqual("name", self.VandyImpl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.VandyImpl.createSourceName("", "file:////name"))

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testCreateOutputDirectory(self, mock_executeCommand):
        self.VandyImpl.createOutputDirectory("file/dir")
        mock_executeCommand.assert_called_with(self.VandyImpl._mkdirScript + " file")

    def testCreateStageOutCommand_stageInFalse(self):
        self.VandyImpl.stageIn = False
        self.assertEqual(self.VandyImpl._cpScript + " sourcePFN targetPFN",
                         self.VandyImpl.createStageOutCommand("sourcePFN", "targetPFN"))

    def testCreateStageOutCommand_stageInTrue(self):
        self.VandyImpl.stageIn = True
        self.assertEqual(self.VandyImpl._downloadScript + " sourcePFN targetPFN",
                         self.VandyImpl.createStageOutCommand("sourcePFN", "targetPFN"))

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.VandyImpl.removeFile("file")
        mock_executeCommand.assert_called_with(self.VandyImpl._rmScript + " file")
