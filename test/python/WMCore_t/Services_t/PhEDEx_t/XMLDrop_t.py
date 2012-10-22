#!/usr/bin/env python

import time
import os
import re
import unittest
import logging

from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMQuality.TestInit import TestInit

from WMCore.Services.PhEDEx import XMLDrop


class XMLDropTest(unittest.TestCase):
    """
    _XMLDropTest_

    Test the XML Drop class.

    I really have no idea what that class does, so you're getting a best guess.
    """

    def setUp(self):
        """
        _setUp_

        Do nothing
        """
        self.dbsURL = "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"

        return

    def tearDown(self):
        """
        _tearDown_

        Do nothing
        """

        return


    def testA_XMLDrop(self):
        """
        _XMLDrop_

        Um...test that it does what it does?
        """
        datasetPath = "/Cosmics/CRUZET09-PromptReco-v1/RECO"
        fileBlockName = "/Cosmics/CRUZET09-PromptReco-v1/RAW#1"

        spec        = XMLDrop.XMLInjectionSpec(self.dbsURL)
        datasetSpec = spec.getDataset(datasetPath)
        fileBlock   = datasetSpec.getFileblock(fileBlockName)
        fileBlock.addFile("lfn", {'adler32': '201', 'cksum': '101'}, '100')

        output = spec.save()
        self.assertTrue(re.search('<data version="2">', output) > 0)
        self.assertTrue(re.search('<dbs dls="dbs" name="%s">' % self.dbsURL, output) > 0)
        self.assertTrue(re.search('<dataset is-open="y" is-transient="n" name="%s">' % datasetPath, output) > 0)
        self.assertTrue(re.search('<block is-open="y" name="%s">' % fileBlockName, output) > 0)
        self.assertTrue(re.search('<file bytes="100" checksum="adler32:201,cksum:101" name="lfn"/>', output) > 0)
        self.assertTrue(re.search('</block>', output) > 0)
        self.assertTrue(re.search('</dataset>', output) > 0)



        return

if __name__ == '__main__':
    unittest.main()
