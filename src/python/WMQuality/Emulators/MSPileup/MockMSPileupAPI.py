#!/usr/bin/env python
"""
Version of WMCore/Services/MSPileup intended to be used with mock or unittest.mock
"""


def getPileupDocs(mspileupUrl, queryDict=None, method='GET'):
    """
    Returns list of Pileup Documents.
    """
    print(f"Mocking MSPileup getPileupDocs: \
          url: {mspileupUrl}, query: {queryDict}, method: {method}")

    queryDict = queryDict or {}

    pdict = {
        # "pileupName": "/MinBias_TuneCP5_14TeV-pythia8/PhaseIITDRSpring19GS-106X_upgrade2023_realistic_v2_ext1-v1/GEN-SIM",
        "pileupName": "/GammaGammaToEE_Elastic_Pt15_8TeV-lpair/Summer12-START53_V7C-v1/GEN-SIM",
        "pileupType": "classic",
        "insertTime": 1680873642,
        "lastUpdateTime": 1706216047,
        "expectedRSEs": [
            "T2_XX_SiteA",
            "T2_XX_SiteB",
            "T2_XX_SiteC"
            ],
        "currentRSEs": [
            "T2_XX_SiteA",
            "T2_XX_SiteB",
            "T2_XX_SiteC"
            ],
        "fullReplicas": 1,
        "campaigns": [
            "Apr2023_Val"
            ],
        "containerFraction": 1.0,
        "replicationGrouping": "ALL",
        "activatedOn": 1706216047,
        "deactivatedOn": 1680873642,
        "active": True,
        "pileupSize": 1233099715874,
        "ruleIds": [
            "55e5a21aecb5445c8aa40581a7bf18d2",
            "67a3fa7252f54507ba1c45f271beb754"
            ],
        "customName": "",
        "transition": []}

    if 'filters' in queryDict.keys():
        return [{k: v for k, v in pdict.items() if k in queryDict['filters']}]

    return [pdict]
