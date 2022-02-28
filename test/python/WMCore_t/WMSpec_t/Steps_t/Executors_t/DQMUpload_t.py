from __future__ import print_function
import logging
from future import standard_library
standard_library.install_aliases()
import urllib.request

import unittest
from nose.plugins.attrib import attr

import os
from functools import reduce
from hashlib import md5

from WMCore.WMSpec.Steps.Executors.DQMUpload import DQMUpload

class StepPatch():
    def __init__(self, upload=None):
        self.upload = upload

class UploadPatch():
    def __init__(self):
        self.proxy = False

class DQMUpload_t(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @attr("integration")
    def test_upload(self):
        """
        test_upload

        This unittest has been used to validate the modernization of DQMUpload.
        If everything is ok, this unittest is expected to pass.

        However, this unittest sends a 144MB root file to the DQM. We convened 
        that it is a bit too much for multiple daily checks, so we mark this
        as "integration". See discussion at:
        https://github.com/dmwm/WMCore/pull/9934#issuecomment-898855725
        """
        filename = "DQM_V0001_R000000001__RelValH125GGgluonfusion_13__CMSSW_8_1_0-RecoFullPU_2017PU_TaskChain_PUMCRecyc_HG1705_Validation_TEST_Alan_v67-v11__DQMIO.root"
        filepath = os.path.join("/tmp", filename)
        if not os.path.isfile(filepath):
            urllib.request.urlretrieve(
                "http://amaltaro.web.cern.ch/amaltaro/forAlan/%s" % filename, 
                filepath)

        dqm = DQMUpload()
        upload_patch = UploadPatch()
        step_patch = StepPatch(upload_patch)
        dqm.step = step_patch

        args = {}
        # Preparing a checksum
        blockSize = 0x10000
        def upd(m, data):
            m.update(data)
            return m
        with open(filepath, 'rb') as fd:
            contents = iter(lambda: fd.read(blockSize), b'')
            m = reduce(upd, contents, md5())
        args['checksum'] = 'md5:%s' % m.hexdigest()
        args['size'] = os.path.getsize(filepath)
        headers, data = dqm.upload(
            "https://cmsweb-testbed.cern.ch/dqm/dev/", 
            args,
            filepath)

        logging.debug("headers: %s %s", type(headers), headers)
        logging.debug("data: %s %s", type(data), data)

        # Dqm-Status-Code == 100 is considered good
        # Dqm-Status-Code == 300 was related to " File name does not match the expected convention"
        self.assertEqual(headers.get("Dqm-Status-Code", None), '100')

    @attr("integration")
    def test_httppost(self):
        """
        test_httppost

        This unittest has been used to validate the modernization of DQMUpload.
        If everything is ok, this unittest is expected to pass.
        """
        filename = "DQM_V0001_R000000001__RelValH125GGgluonfusion_13__CMSSW_8_1_0-RecoFullPU_2017PU_TaskChain_PUMCRecyc_HG1705_Validation_TEST_Alan_v67-v11__DQMIO.root"
        filepath = os.path.join("/tmp", filename)
        if not os.path.isfile(filepath):
            urllib.request.urlretrieve(
                "http://amaltaro.web.cern.ch/amaltaro/forAlan/%s" % filename, 
                filepath)

        dqm = DQMUpload()
        upload_patch = UploadPatch()
        step_patch = StepPatch(upload_patch)
        dqm.step = step_patch
        dqm.step.upload.URL = "https://cmsweb-testbed.cern.ch/dqm/dev/"
        dqm.httpPost(filepath)

if __name__ == '__main__':
    unittest.main()
