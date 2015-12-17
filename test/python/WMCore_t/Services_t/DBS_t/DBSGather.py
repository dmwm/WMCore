#!/usr/bin/env python
"""
_DBSGather_t_

Unit test for the DBS helper class.
"""

import json

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from WMCore.Services.DBS.DBSReader import DBSReader as DBSReader

lookup = {}

try:
    with open('DBSMockData.json', 'r') as mockData:
        lookup = json.load(mockData)
except IOError:
    lookup = {}

datasets = ['/HighPileUp/Run2011A-v1/RAW']  # Datasets to get all blocks, array, and lumis from

calls = [['listDataTiers'],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'}],
         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace', 'detail': True}],
         ['listBlocks', {'detail': True, 'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listBlocks', {'detail': False, 'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listBlocks',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0',
           'detail': True}],
         ['listBlocks',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0'}],
         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace', 'detail': True,
           'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listRuns', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'}],
         ['listRuns', {'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listFileArray', {'validFileOnly': 1, 'detail': True, 'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listFileArray', {'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listFileArray',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0',
           'detail': True, 'validFileOnly': 1}],
         ['listFileArray',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace', 'detail': False,
           'validFileOnly': 1}],
         ['listFileArray',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace', 'detail': True,
           'validFileOnly': 1}],
         ['listFileArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root',
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/04FBE4D8-2DCF-E411-B827-02163E0124D5.root',
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root',
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/FA4E40C0-2DCF-E411-AD1A-02163E012186.root'],
             'detail': True}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'RAW'}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'RAW', 'detail': True}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'blah'}],
         ['listDatasets', {'primary_ds_name': 'blah', 'data_tier_name': 'RAW'}],
         ['listFileLumiArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root',
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/04FBE4D8-2DCF-E411-B827-02163E0124D5.root',
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root',
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/FA4E40C0-2DCF-E411-AD1A-02163E012186.root']}],
         ['listFileLumis',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace', 'validFileOnly': 1}],
         ['listFileLumis',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0',
           'validFileOnly': 1}],
         ['listFileParents',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0'}],
         ['listFileSummaries', {'validFileOnly': 1, 'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listFileSummaries',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas', 'validFileOnly': 1}],
         ['listFileSummaries', {'validFileOnly': 1, 'dataset': '/HighPileUp/Run2011A-v1/RAWblah'}],
         ['listFileSummaries',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace', 'validFileOnly': 1}],
         ['listPrimaryDatasets', {'primary_ds_name': 'Jet*'}],
         ['listPrimaryDatasets', {'primary_ds_name': 'DoesntExist'}],
         ['listBlockParents',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0'}],
         ['listBlockParents', {'block_name': '/Cosmics/Commissioning2015-v1/RAW#942d76fe-cf0e-11e4-afad-001e67ac06a0'}],

         # Exception throwing calls

         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas', 'detail': True}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#somethingelse'}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas'}],
         ['listBlocks', {'detail': True, 'dataset': '/HighPileUp/Run2011A-v1/RAWblah'}],
         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas', 'detail': True,
           'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#blah'}],
         ]

# These three lines will generate the the needed phys03 data instead of global data (and change the filename)

# calls = [
#     ['listBlockOrigin', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caabcd'}],
#     ['listBlockOrigin', {
#         'block_name': '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-ea0972193530f531086947d06eb0f121/USER#fb978442-a61b-413a-b4f4-526e6cdb142e'}],
#     ['listBlockOrigin', {
#         'block_name': '/GenericTTbar/hernan-140317_231446_crab_JH_ASO_test_T2_ES_CIEMAT_5000_100_140318_0014-ea0972193530f531086947d06eb0f121/USER#0b04d417-d734-4ef2-88b0-392c48254dab'}],
# ]
# datasets = []
# endpoint = 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'

endpoint = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
dbs = DBSReader(endpoint)
realDBS = dbs.wrapped.dbs

for dataset in datasets:
    blocks = realDBS.listBlocks(dataset=dataset)
    for block in blocks:
        calls.append(['listBlocks', {'block_name': unicode(block['block_name'])}])
        calls.append(['listBlocks', {'block_name': unicode(block['block_name']), 'detail': True}])
        calls.append(['listFileLumis', {'block_name': unicode(block['block_name'])}])
        calls.append(['listFileLumis', {'block_name': unicode(block['block_name']), 'validFileOnly': 1}])
        calls.append(
            ['listFileArray', {'block_name': unicode(block['block_name']), 'detail': True, 'validFileOnly': 1}])

for call in calls:
    func = getattr(realDBS, call[0])
    if len(call) > 1:
        signature = '%s:%s' % (call[0], sorted(call[1].iteritems()))
        try:
            result = func(**call[1])
        except HTTPError:
            result = 'Raises HTTPError'
    else:
        result = func()
        signature = call[0]

    lookup.update({signature: result})

with open('DBSMockData.json', 'w') as mockData:
    json.dump(lookup, mockData, indent=1)


