from future import standard_library
standard_library.install_aliases()

import logging
import os
import unittest
from http.client import HTTPException

from WMCore.Services.Requests import Requests
from WMCore.WMBase import getTestBase
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig


class PyCurlRESTServer(RESTBaseUnitTest):
    """
    Loads the rest server and test the upload
    """

    def initialize(self):
        self.config = DefaultConfig('WMCore_t.Services_t.PyCurlRESTModel')
        self.config.Webtools.environment = 'development'
        self.config.Webtools.error_log_level = logging.ERROR
        self.config.Webtools.access_log_level = logging.ERROR
        self.config.Webtools.port = 8888
        self.config.Webtools.host = '127.0.0.1'
        self.config.UnitTests.object = 'WMCore_t.Services_t.PyCurlRESTModel'
        self.requestHandler = Requests('http://127.0.0.1:8888/unittests/rest/')

    def testFileUpload(self):
        """
        The method upload a file (data/TestUpload.txt) and check if the server API has saved it
        """
        uploadedFilename = 'UploadedFile.txt'
        fileName = os.path.join(getTestBase(), 'WMCore_t/Services_t/TestUpload.txt')
        # Check the uploaded file is not there
        if os.path.isfile(uploadedFilename):
            os.remove(uploadedFilename)
            self.assertFalse(os.path.isfile(uploadedFilename))
        # do the upload
        res = self.requestHandler.uploadFile(fileName, 'http://127.0.0.1:8888/unittests/rest/file/')
        # the file is there now (?)
        self.assertTrue(os.path.isfile(uploadedFilename))
        self.assertEqual(open(uploadedFilename).read(), open(fileName).read())
        # delete the uploaded file
        os.remove(uploadedFilename)
        self.assertTrue(b'Success' in res)

    def testFailingFileUpload(self):
        """
        The method upload a file (data/TestUpload.txt) and check if the server API has saved it
        """
        uploadedFilename = 'UploadedFile.txt'
        fileName = os.path.join(getTestBase(), 'WMCore_t/Services_t/TestUpload.txt')
        # Check the uploaded file is not there
        if os.path.isfile(uploadedFilename):
            os.remove(uploadedFilename)
            self.assertFalse(os.path.isfile(uploadedFilename))
        # do the upload using the wrong address
        try:
            res = self.requestHandler.uploadFile(fileName, 'http://127.0.0.1:8888/unittests/rest/iAmNotThere/')
            self.fail()
        except HTTPException as he:
            self.assertEqual(he.status, 404)
        self.assertFalse(os.path.isfile(uploadedFilename))


if __name__ == "__main__":
    unittest.main()
