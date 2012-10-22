#!/usr/bin/python

"""
_LoggingInfoParser_t__

LoggingInfoParser unittests
"""
import unittest
import os

from WMCore.BossAir.Plugins.GLiteLIParser import LoggingInfoParser
from WMCore.WMBase import getTestBase

class LoggingInfoParserTest(unittest.TestCase):
    def testParseFile(self):
        expected = [
                  'Failed to create a delegation id for job https://wms314.cern.ch:9000/ZxU-l9vqfPio83wOBexrEw: reason is Received NULL fault; the error is due to another cause: FaultString=[] - FaultCode=[SOAP-ENV:Server.generalException] - FaultSubCode=[SOAP-ENV:Server.generalException] - FaultDetail=[<faultData><ns1:MethodName xmlns:ns1="http://glite.org/2007/11/ce/cream/types">invoke</ns1:MethodName><ns2:Timestamp xmlns:ns2="http://glite.org/2007/11/ce/cream/types">2011-10-06T14:48:40.464Z</ns2:Timestamp><ns3:ErrorCode xmlns:ns3="http://glite.org/2007/11/ce/cream/types">0</ns3:ErrorCode><ns4:Description xmlns:ns4="http://glite.org/2007/11/ce/cream/types">Missing property local.user.id</ns4:Description><ns5:FaultCause xmlns:ns5="http://glite.org/2007/11/ce/cream/types">Missing property local.user.id</ns5:FaultCause></faultData><ns6:exceptionName xmlns:ns6="http://xml.apache.org/axis/">org.glite.ce.creamapi.ws.cream2.types.AuthorizationFault</ns6:exceptionName><ns7:hostname xmlns:ns7="http://xml.apache.org/axis/">europa.hep.kbfi.ee</ns7:hostname>]',
                  'BrokerHelper: no compatible resources',
                  'Cannot upload file:///home/cmsint/globus-tmp.alicegrid45.4449.0/https_3a_2f_2fwms202.cern.ch_3a9000_2faK4h7l2QlbIgohdxEC7icg/Report.1.pkl into gsiftp://crabas.lnl.infn.it/data/wmagent/osb/JobCache/mmascher_crab_MyAnalysis___110405_094333/Analysis/JobCollection_4_0/job_16/Report.1.pkl',
                  '7 authentication with the remote server failed',
                  'Cannot move ISB (retry_copy ${globus_transfer_cmd} gsiftp://crabas.lnl.infn.it/home/riahi/ASYNCHDEPLOY/v01/install/wmagent/WorkQueueManager/cache/mmascher_crab_asynctestFLORIDA_110920_004920/batch_26-2/JobPackage.pkl file:///home/cms509/home_cream_172602428/CREAM172602428/JobPackage.pkl): error: globus_ftp_client: the server responded with an error500 500-Command failed. : globus_l_gfs_file_open failed.500-globus_xio: Unable to open file /home/riahi/ASYNCHDEPLOY/v01/install/wmagent/WorkQueueManager/cache/mmascher_crab_asynctestFLORIDA_110920_004920/batch_26-2/JobPackage.pkl500-globus_xio: System error in open: Permission denied500-globus_xio: A system call failed: Permission denied500 End.',
                  'Cannot download JobPackage.pkl from gsiftp://crabas.lnl.infn.it/home/riahi/ASYNCHDEPLOY/v01/install/wmagent/WorkQueueManager/cache/mmascher_crab_asynctestFLORIDA_110920_004920/batch_26-1/JobPackage.pkl',
                  'Cannot move OSB (${globus_transfer_cmd} file:///home/cms509/home_cream_443527690/CREAM443527690/199_0.stderr gsiftp://crabas.lnl.infn.it/home/riahi/deploys/CRAB304bis/v01/install/wmagent/JobCreator/JobCache/mmascher_crab_primo_bisi2_111018_181635/Analysis/JobCollection_2_0/job_199/199_0.stderr): error: globus_ftp_client: the server responded with an error500 500-Command failed. : globus_l_gfs_file_open failed.500-globus_xio: Unable to open file /home/riahi/deploys/CRAB304bis/v01/install/wmagent/JobCreator/JobCache/mmascher_crab_primo_bisi2_111018_181635/Analysis/JobCollection_2_0/job_199/199_0.stderr500-globus_xio: System error in open: Permission denied500-globus_xio: A system call failed: Permission denied500 End.; Cannot move OSB (${globus_transfer_cmd} file:///home/cms509/home_cream_443527690/CREAM443527690/199_0.stderr gsiftp://crabas.lnl.infn.it/home/riahi/deploys/CRAB304bis/v01/install/wmagent/JobCreator/JobCache/mmascher_crab_primo_bisi2_111018_181635/Analysis/JobCollection_2_0/job_199/199_0.stderr): error: globus_ftp_client: the server responded with an error 500 500-Command failed. : globus_l_gfs_file_open failed.  500-globus_xio: Unable to open file /home/riahi/deploys/CRAB304bis/v01/install/wmagent/JobCreator/JobCache/mmascher_crab_primo_bisi2_111018_181635/Analysis/JobCollection_2_0/job_199/199_0.stderr  500-globus_xio: System error in open: Permission denied  500-globus_xio: A system call failed: Permission denied  500 End.',
                  'BLAH error: submission command failed (exit code = 1) (stdout:) (stderr:pbs_iff: cannot read reply from pbs_server-No Permission.-qsub: cannot connect to server gaebatch.ciemat.es (errno=15007) Unauthorized Request -) N/A (jobId = CREAM008087355)',
                  'CREAM Register raised std::exception The endpoint is blacklisted',
                  'BrokerHelper: no compatible resources',
                  ]

        lip = LoggingInfoParser()
        i = 0
        for exp in expected:
            fileName = os.path.join(getTestBase(), "WMCore_t/BossAir_t/loggingInfo/loggingInfo.%s.log" % i)
            i += 1
            res = lip.parseFile(fileName)
            self.assertEqual(res, exp)

if __name__ == '__main__':
    unittest.main()
