"""
MockReqMgrAux class provides mocking methods of ReqMgrAux class
"""
# futures
from __future__ import (division, print_function)
from builtins import object

# system modules
import time

# WMCore modues
from WMCore.Agent.DefaultConfig import DEFAULT_AGENT_CONFIG


class MockReqMgrAux(object):
    """
    Version of Services/PhEDEx intended to be used with mock or unittest.mock
    """

    def __init__(self, *args, **kwargs):
        print("Using MockReqMgrAux", args, kwargs)
        tstamp = int(time.time())
        cname = 'testCampaign'
        cdict = {"CampaignName": cname,
                 "PrimaryAAA": True,
                 "SecondaryAAA": False,
                 "SiteWhiteList": ["T1", "T2"],
                 "SiteBlackList": [],
                 "SecondaryLocation": ["T1", "T2"],
                 "Secondaries": {"datasetA": ["T1", "T2"], "datasetB": ["T1"]},
                 "MaxCopies": 2,
                 "TiersToDM": [],
                 "PartialCopy": 1
                 }
        self.campaigns = [cdict]
        self.transferRecords = []
        rec = {"dataset": "/a/b/c", "dataType": "primary",
               "transferIDs": [1, 2], "completion": [1], "campaignName": cname}
        self.transferRecords = [{'workflowName': 'test', 'lastUpdate': tstamp, 'transfers': [rec]}]

    def getWMAgentConfig(self, agentName):
        """
        macking getWMAgentConfig returns default config.
        """
        return DEFAULT_AGENT_CONFIG

    def getCampaignConfig(self, name):
        "mocking getCampaignConfig method"
        campaigns = []
        if name == 'ALL_DOCS':
            return self.campaigns
        for camp in self.campaigns:
            if camp['CampaignName'] == name:
                campaigns.append(camp)
        return campaigns

    def getTransferInfo(self, name):
        "mocking getTransferInfo method"
        if name == 'ALL_DOCS':
            return self.transferRecords
        records = []
        for rec in self.transferRecords:
            if rec['workflowName'] == name:
                records.append(rec)
        return records

    def updateTransferInfo(self, name, doc, inPlace=False):
        "mocking updateTransferInfo method"
        # while mocking we do nothing
        print('mock method updateTransferInfo: name=%s, doc=%s inPlace=%s' \
              % (name, doc, inPlace))
