#!/usr/bin/env python
"""
_DBSError_t_

Unit test for the DBSError class.
"""

import unittest

# from nose.plugins.attrib import attr

from dbs.apis.dbsClient import DbsApi
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from WMCore.Services.DBS.DBSErrors import DBSError


class DBSErrorsTest(unittest.TestCase):
    """
    DNSErrorsTest class
    """

    def setUp(self):
        """
        _setUp_
        Initialize unit test data
        """

        url = "https://cmsweb-testbed.cern.ch:8443/dbs/int/global/DBSWriter"
        self.api = DbsApi(url=url, useGzip=True, aggregate=False)

        # provide mailformed data to insertBulkBlock API
        files = [
                 {"check_sum": "1504266448",
                  "file_lumi_list": [{"lumi_section_num": 27414, "run_num": 1}],
                  "adler32": "NOTSET",
                  "event_count": 1619,
                  "file_type": "EDM",
                  "last_modified_by": "Test",
                  "logical_file_name": "/store/data/a/b/A/a/1/abcd9.root",
                  "file_size": 123,
                  "last_modification_date": 1279912089,
                  "auto_cross_section": 0.0}]
        self.data = {'files': files}

    def testErrors(self):
        """
        testErrors tests DBSError class
        """
        data = """[{"error": {"code":123, "message": "message", "reason":"reason"}, "http": {"code": 500}}]"""
        dbsError = DBSError(data)
        httpCode = dbsError.getHttpCode()
        srvCode = dbsError.getServerCode()
        msg = dbsError.getMessage()
        reason = dbsError.getReason()
        self.assertTrue(srvCode == 123)
        self.assertTrue(httpCode == 500)
        self.assertTrue(msg == 'message')
        self.assertTrue(reason == 'reason')

    def testDBSError(self):
        """
        testDBSError tests DBSError class
        """
        data = """[{"error":{"reason":"invalid parameter(s)","message":"parameter 'bla' is not accepted by 'datatiers' API","function":"dbs.parameters.CheckQueryParameters","code":118}, "http":{"method":"GET","code":400,"timestamp":"2022-11-10 13:51:27.924928 -0500 EST m=+40.711338056","path":"/dbs2go/datatiers?bla=1","user_agent":"curl/7.86.0","x_forwarded_host":"","x_forwarded_for":"","remote_addr":"127.0.0.1:56191"},"exception":400,"type":"HTTPError","message":"DBSError Code:118 Description:DBS invalid parameter for the DBS API Function:dbs.parameters.CheckQueryParameters Message:parameter 'bla' is not accepted by 'datatiers' API Error: invalid parameter(s)"}]"""
        dbsError = DBSError(data)
        srvCode = dbsError.getServerCode()
        httpCode = dbsError.getHttpCode()
        msg = dbsError.getMessage()
        reason = dbsError.getReason()
        self.assertTrue(srvCode == 118)
        self.assertTrue(httpCode == 400)
        self.assertTrue(msg == "parameter 'bla' is not accepted by 'datatiers' API")
        self.assertTrue(reason == "invalid parameter(s)")

    def testDBSErrorViaDBSClient(self):
        """
        testDBSErrorViaDBSClient tests DBSError class
        """
        # results should contain an error
        # here expected message and reason taken from actual DBS error
        expectedReason = 'DBSError Code:110 Description:DBS DB insert record error Function:dbs.GetRecID Message: Error: nested DBSError Code:113 Description:DBS validation error, e.g. input parameter does not match lexicon rules Function:dbs.primarydstypes.Insert Message: Error: nested DBSError Code:115 Description:DBS decode record failure, e.g. mailformed JSON Function:dbs.DecodeValidatorError Message: Error: validation error'
        expectedMessage = "4106c5ea3bcefee148a7ec69592baa5ec1f1849a240e3af15de850f8e6e5fd81 unable to find primary_ds_type_id for primary ds type=''"
        try:
            self.api.insertBulkBlock(self.data)
        except HTTPError as exp:
            #print("### HTTPError exception: %s" % exp)
            #print("### HTTPError exception representation: %s" % repr(exp))
            dbsError = DBSError(exp)
            srvCode = dbsError.getServerCode()
            httpCode = dbsError.getHttpCode()
            msg = dbsError.getMessage()
            reason = dbsError.getReason()
            self.assertTrue(srvCode == 109)
            self.assertTrue(httpCode == 400)
            self.assertTrue(msg == expectedMessage)
            self.assertTrue(reason == expectedReason)

    def testDBSErrorException(self):
        """
        testDBSErrorException tests DBSError class
        """
        with self.assertRaises(HTTPError):
            self.api.insertBulkBlock(self.data)


if __name__ == '__main__':
    unittest.main()
