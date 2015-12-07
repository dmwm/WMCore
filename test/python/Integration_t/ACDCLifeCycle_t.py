from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t, recordException

from WMCore.ACDC.AnalysisCollectionService import AnalysisCollectionService
from WMCore.Wrappers.JsonWrapper import loads
from WMCore.WMBase import getTestBase
from WMCore.Database.CMSCouch import Database

import os
import time
import unittest
import nose
from nose.plugins.attrib import attr


class ACDCLifeCycle_t(RequestLifeCycleBase_t, unittest.TestCase):
    """Submit and track an acdc request

        Note: This test needs a previous request to be in reqmgr for it to clone
    """

    requestParams = {   'RequestType' : 'Resubmission',
                        'OriginalRequestName' : 'integration_RequestCancellation_t_121004_164419_7487',
                        'InitialTaskPath' : '/%s/DataProcessing',
                        'ACDCDatabase' : 'wmagent_acdc',
                        'Requestor' : 'integration', 'Group' : 'DMWM'
                    }

    @attr("lifecycle")
    @recordException
    def test06UploadACDC(self):
        # get previous request we can piggyback on
        for request in reversed(self.__class__.reqmgr.getRequest()):
            request = request['WMCore.RequestManager.DataStructs.Request.Request']['RequestName']
            if 'RequestCancellation_t' in request:
                self.__class__.requestParams['OriginalRequestName'] = request
                break
        else:
            raise nose.SkipTest("no suitable request in reqmgr to resubmit")

        self.__class__.requestParams['InitialTaskPath'] = self.__class__.requestParams['InitialTaskPath'] % self.__class__.requestParams['OriginalRequestName']
        self.__class__.requestParams['ACDCServer'] = self.__class__.endpoint + '/couchdb'

        # create and upload acdc
        service = AnalysisCollectionService(url=self.__class__.endpoint + '/couchdb', database = 'wmagent_acdc')
        service.createCollection(self.__class__.requestParams['OriginalRequestName'], 'integration', 'DMWM')
        with open(os.path.join(getTestBase(), '..', 'data', 'ACDC', 'linacre_ACDC_ReReco13JulCosmics_120809_130020_117_120823_200309_5735.json')) as infile:
            acdc_json = infile.read().replace('linacre_ACDC_ReReco13JulCosmics_120809_130020_117_120823_200309_5735',
                                              self.__class__.requestParams['OriginalRequestName'])
        acdc_json = loads(acdc_json)
        acdc_database = Database('wmagent_acdc', self.__class__.endpoint + '/couchdb')
        acdc_database.commit(acdc_json)

# Remove tests that don't apply
delattr(RequestLifeCycleBase_t, 'test05InjectConfigs')

if __name__ == "__main__":
    unittest.main()
