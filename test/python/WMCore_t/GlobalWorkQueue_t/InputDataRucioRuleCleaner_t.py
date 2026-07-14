from WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner import InputDataRucioRuleCleaner
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

import json
import unittest

from mock import mock


class DummyREST:
    def __init__(self):
        self.logger = None
        self.config = None


# MSRuleCleaner requires a plain dict while CherryPyPeriodicTask requires attribute access
class DictWithAttrs(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'") from e


class InputDataRucioRuleCleanerTest(EmulatedUnitTestCase):

    def setUp(self):
        # --- Why we mock TagCollector before super().setUp() ---
        #
        # EmulatedUnitTestCase.setUp() sets up several patchers, e.g.:
        #   mock.patch('WMCore.ReqMgr.Tools.cms.CRIC', ...)
        #   mock.patch('WMCore_t.WMSpec_t.Steps_t.Fetchers_t.PileupFetcher_t.Rucio', ...)
        #
        # Setting up each patcher causes Python to import the target module for the
        # first time. Both of the above targets cause cms.py to be imported, and cms.py
        # has this at module level:
        #
        #   TC = TagCollector()   # cms.py line 18
        #
        # TagCollector.__init__ tries to load SSL certificates, which fails in a
        # test environment without real certs.
        #
        # By replacing the TagCollector *class* with a MagicMock before super().setUp()
        # runs, the first import of cms.py instantiates the mock instead of the real
        # class, so no SSL calls are made.
        #
        # --- Why we configure releases/architectures return values ---
        #
        # WMSpecGenerator.createReRecoSpec() calls StdBase.getTestArguments(), which
        # picks a test CMSSWVersion and ScramArch. Those values are then validated:
        #
        #   "validate": lambda x: x in releases()   # StdBase.py
        #
        # releases() calls TC.releases(), and TC is the MagicMock instance created
        # above (tagCollectorMock.return_value). Without configuring the return value,
        # TC.releases() returns a bare MagicMock and 'x' in MagicMock() evaluates to
        # False, failing validation.
        #
        # We use _AnyContains — a list subclass whose __contains__ always returns True —
        # so that any CMSSW version or ScramArch passes validation, regardless of what
        # getTestArguments() returns. This avoids brittle hardcoding that would break
        # if StdBase.getTestArguments() is updated to use a newer release.
        #
        # Mock hierarchy:
        #   tagCollectorMock              — the mocked class
        #   tagCollectorMock()            — instantiating it → tagCollectorMock.return_value
        #   TC = TagCollector()           → TC = tagCollectorMock.return_value
        #   TC.releases()                 → tagCollectorMock.return_value.releases.return_value
        class _AnyContains(list):
            def __contains__(self, item):
                return True

        from unittest.mock import MagicMock
        tagCollectorMock = MagicMock()
        tagCollectorMock.return_value.releases.return_value = _AnyContains()
        tagCollectorMock.return_value.architectures.return_value = _AnyContains()
        tagCollectorPatcher = mock.patch(
            'WMCore.Services.TagCollector.TagCollector.TagCollector', new=tagCollectorMock)
        tagCollectorPatcher.start()
        self.addCleanup(tagCollectorPatcher.stop)

        super(InputDataRucioRuleCleanerTest, self).setUp()

        from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator

        self.msRuleCleanerConfig = {
            "verbose": True,
            "interval": 1 * 60,
            "services": ['ruleCleaner'],
            "rucioAccount": 'wma_test',
            'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
            'msOutputUrl': 'https://cmsweb-testbed.cern.ch/ms-output',
            'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
            'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
            'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
            'rucioUrl': 'http://cms-rucio-int.cern.ch',
            'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
            "wmstatsUrl": "https://cmsweb-testbed.cern.ch/wmstatsserver",
            "logDBUrl": "https://cmsweb-testbed.cern.ch/couchdb/wmstats_logdb",
            'logDBReporter': 'reqmgr2ms_ruleCleaner',
            'archiveDelayHours': 8,
            'archiveAlarmHours': 24,
            'enableRealMode': True,
        }

        self.specGenerator = WMSpecGenerator("WMSpecs")
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=[], useDefault=False)
        for dbName in ('workqueue_t', 'workqueue_t_inbox', 'local_workqueue_t', 'local_workqueue_t_inbox'):
            self.testInit.setupCouch(dbName, "WorkQueue")
        self.testInit.generateWorkDir()

        self.msRuleCleanerConfig['QueueURL'] = self.testInit.couchUrl

        self.queueParams = {
            'log_reporter': "Services_WorkQueue_Unittest",
            'rucioAccount': self.msRuleCleanerConfig['rucioAccount'],
            'rucioAuthUrl': "http://cms-rucio-int.cern.ch",
            'rucioUrl': "https://cms-rucio-auth-int.cern.ch",
            '_internal_name': 'GlobalWorkQueueTest',
            'log_file': 'test.log',
        }

        self.config_obj = DictWithAttrs()
        self.config_obj._internal_name = "GlobalWorkQueueTest"
        self.config_obj.log_file = "test.log"
        self.config_obj.queueParams = self.queueParams
        self.config_obj.msRuleCleaner = self.msRuleCleanerConfig
        self.config_obj.cleanInputDataRucioRuleDuration = 10

    def _makeCleaner(self):
        """Create an InputDataRucioRuleCleaner with a fresh GlobalQueue and MockRucioApi."""
        from WMQuality.Emulators.RucioClient.MockRucioApi import MockRucioApi
        cleaner = InputDataRucioRuleCleaner(rest=DummyREST(), config=self.config_obj)
        cleaner.globalQ = globalQueue(DbName='workqueue_t',
                                      QueueURL=self.testInit.couchUrl,
                                      UnittestFlag=True,
                                      logger=cleaner.logger,
                                      **self.queueParams)
        msRuleCleaner = MSRuleCleaner(self.config_obj.msRuleCleaner, logger=cleaner.logger)
        msRuleCleaner.resetCounters()
        msRuleCleaner.rucio = MockRucioApi(self.msRuleCleanerConfig['rucioAccount'])
        cleaner.msRuleCleaner = msRuleCleaner
        return cleaner

    def _getWorkflowElements(self, wqService, workflowName):
        """Return raw CouchDB view rows for the given workflow."""
        return wqService.db.loadView(
            'WorkQueue', 'elementsDetailByWorkflowAndStatus',
            {'startkey': [workflowName], 'endkey': [workflowName, {}], 'reduce': False}
        )['rows']

    @mock.patch('WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner.InputDataRucioRuleCleaner.getRequestForInputDataset')
    def testInputDataRucioRuleCleaner(self, mock_getRequestForInputDataset):
        """
        Single-workflow happy path:
          - Done element at <100% is skipped.
          - Done element at 100%/100% with a Rucio rule gets the rule cleaned.
          - The element is added to the skip-set and skipped on the next cycle.
        """
        specName = "RerecoSpec"
        inputDataset = "/JetHT/Run2012C-v1/RAW"

        specUrl = self.specGenerator.createReRecoSpec(
            specName, "file",
            assignKwargs={'SiteWhitelist': ["T2_XX_SiteA"]},
            InputDataset=inputDataset)

        cleaner = self._makeCleaner()
        cleaner.globalQ.queueWork(specUrl, specName, "teamA")

        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        rows = self._getWorkflowElements(wqService, specName)
        element_id = rows[0]['id']
        blockName = list(rows[0]['value']['Inputs'].keys())[0]
        print("Elements in GlobalQueue:", json.dumps(rows, indent=2))

        # Mock returns only the current request — canDeleteRucioRule skips self and allows deletion
        mock_getRequestForInputDataset.return_value = {
            "result": [{specName: {"RequestName": specName, "RequestStatus": "running-open"}}]
        }

        # Done element at 50%/50% must be skipped
        wqService.updateElements(element_id, PercentComplete=50, PercentSuccess=50, Status='Done')
        self.assertFalse(cleaner.cleanRucioRules(self.config_obj),
                         "Should not clean rules for a Done element with less than 100% completion")

        # Now fully complete — create a mock Rucio rule to be cleaned
        wqService.updateElements(element_id, PercentComplete=100, PercentSuccess=100, Status='Done')
        rule_id = cleaner.msRuleCleaner.rucio.createReplicationRule(
            names=blockName,
            rseExpression="T2_US_Nebraska",
            copies=1,
            account=self.msRuleCleanerConfig['rucioAccount'],
        )[0]
        print("Created Rucio rule with ID:", rule_id)
        print("Rule info:", cleaner.msRuleCleaner.rucio.getRule(rule_id))

        rows = self._getWorkflowElements(wqService, specName)
        print("Updated Elements in GlobalQueue:")
        for e in rows:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])

        self.assertTrue(cleaner.cleanRucioRules(self.config_obj),
                        "cleanRucioRules should return True after cleaning a completed element's rules")

        # Skip-set must be populated and contain only IDs still in the Done queue
        done_ids = {el.id for el in cleaner.globalQ.backend.getElements(status='Done')}
        self.assertTrue(len(cleaner._processedElementIds) > 0,
                        "Skip-set should be non-empty after a successful clean cycle")
        self.assertTrue(cleaner._processedElementIds.issubset(done_ids),
                        "All skip-set IDs should still be present in the Done queue")

        # Second cycle: element already in skip-set, nothing to do
        self.assertFalse(cleaner.cleanRucioRules(self.config_obj),
                         "Second cycle should return False — element already in skip-set")

        # The cleaner sets rule lifetime to 0 via updateRule (not deleteRule),
        # so the mock rule still exists in the store
        self.assertTrue(cleaner.msRuleCleaner.rucio.getRule(rule_id),
                        "Mock rule should still exist after lifetime-0 update (not physically deleted)")

    @mock.patch('WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner.InputDataRucioRuleCleaner.getRequestForInputDataset')
    def testInputDataRucioRuleCleanerTwoWorkflowSameInputdata(self, mock_getRequestForInputDataset):
        """
        Two workflows sharing the same input dataset:
          Cycle 1: specName1 still running  → rule must NOT be cleaned.
          Cycle 2: both workflows at 100%   → rule IS cleaned.
          Cycle 3: specName1 aborted        → rule IS cleaned (aborted is not an active status).
          Cycle 4: specName1 staging with no WQ elements → rule must NOT be cleaned
                   (conservative: staging request with no queue yet blocks deletion).
        """
        specName = "RerecoSpec"
        specName1 = "RerecoSpec1"
        inputDataset = "/JetHT/Run2012C-v1/RAW"

        specUrl = self.specGenerator.createReRecoSpec(
            specName, "file",
            assignKwargs={'SiteWhitelist': ["T2_XX_SiteA"]},
            InputDataset=inputDataset)
        specUrl1 = self.specGenerator.createReRecoSpec(
            specName1, "file",
            assignKwargs={'SiteWhitelist': ["T2_XX_SiteA"]},
            InputDataset=inputDataset)

        cleaner = self._makeCleaner()
        cleaner.globalQ.queueWork(specUrl, specName, "teamA")
        cleaner.globalQ.queueWork(specUrl1, specName1, "teamB")

        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        rows = self._getWorkflowElements(wqService, specName)
        rows1 = self._getWorkflowElements(wqService, specName1)
        print(f"Elements in GlobalQueue {specName}:", json.dumps(rows, indent=2))
        print(f"Elements in GlobalQueue {specName1}:", json.dumps(rows1, indent=2))

        element_id = rows[0]['id']
        blockName = list(rows[0]['value']['Inputs'].keys())[0]

        # Find the specName1 element that covers the same block as specName's first element
        element_id1 = rows1[0]['id']
        for e in rows1:
            if list(e['value']['Inputs'].keys()) == [blockName]:
                element_id1 = e['id']
                break

        # Set specName's element to Done/100%/100% and create a Rucio rule for its block
        wqService.updateElements(element_id, PercentComplete=100, PercentSuccess=100, Status='Done')
        rule_id = cleaner.msRuleCleaner.rucio.createReplicationRule(
            names=blockName,
            rseExpression="T2_US_Nebraska",
            copies=1,
            account=self.msRuleCleanerConfig['rucioAccount'],
        )[0]
        print("Created Rucio rule with ID:", rule_id)
        print("Rule info:", cleaner.msRuleCleaner.rucio.getRule(rule_id))

        # Mutable status dict so each cycle can update statuses without redefining the closure
        statuses = {specName: "running-open", specName1: "running-open"}

        def side_effect(inputdataset, reqmgr2Url):
            if inputdataset == inputDataset:
                return {"result": [{
                    specName:  {"RequestName": specName,  "RequestStatus": statuses[specName]},
                    specName1: {"RequestName": specName1, "RequestStatus": statuses[specName1]},
                }]}
            return {"result": []}

        mock_getRequestForInputDataset.side_effect = side_effect

        # Cycle 1: specName1 element is still at 0%/0% → block deletion
        self.assertFalse(cleaner.cleanRucioRules(self.config_obj),
                         "Rule must not be cleaned while specName1 is still processing the same block")

        # Cycle 2: specName1 element completes → deletion allowed
        wqService.updateElements(element_id1, PercentComplete=100, PercentSuccess=100, Status='Done')
        print(f"Updated Elements in GlobalQueue {specName1}:")
        for e in self._getWorkflowElements(wqService, specName1):
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
        self.assertTrue(cleaner.cleanRucioRules(self.config_obj),
                        "Rule should be cleaned once both workflows have completed the block")

        # Cycle 3: specName1 is aborted (inactive) → deletion allowed even though its element
        # is reset back to incomplete
        wqService.updateElements(element_id1, PercentComplete=0, PercentSuccess=0, Status='Available')
        statuses[specName1] = "aborted"
        cleaner._processedElementIds = set()  # reset so this scenario is evaluated independently
        self.assertTrue(cleaner.cleanRucioRules(self.config_obj),
                        "Rule should be cleaned when the other request is aborted")

        # Cycle 4: specName1 is staging (active) but has no WQ elements → conservative block
        cleaner.globalQ.backend.deleteWQElementsByWorkflow([specName1])
        statuses[specName1] = "staging"
        cleaner._processedElementIds = set()  # reset so this scenario is evaluated independently
        self.assertFalse(cleaner.cleanRucioRules(self.config_obj),
                         "Rule must not be cleaned when a staging request has no queue elements yet")

        # The cleaner sets rule lifetime to 0 via updateRule (not deleteRule),
        # so the mock rule still exists in the store
        self.assertTrue(cleaner.msRuleCleaner.rucio.getRule(rule_id),
                        "Mock rule should still exist after lifetime-0 update (not physically deleted)")


if __name__ == '__main__':
    unittest.main()
