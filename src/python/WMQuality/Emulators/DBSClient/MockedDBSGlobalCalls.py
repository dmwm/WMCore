#!/usr/bin/env python

from __future__ import (division, print_function)

endpoint = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'

# Datasets to get all blocks, array, and lumis from
datasets = ['/HighPileUp/Run2011A-v1/RAW', '/MinimumBias/ComissioningHI-v1/RAW', '/Cosmics/ComissioningHI-v1/RAW',
            '/Cosmics/ComissioningHI-PromptReco-v1/RECO',
            '/SingleElectron/StoreResults-Run2011A-WElectron-PromptSkim-v4-ALCARECO-NOLC-36cfce5a1d3f3ab4df5bd2aa0a4fa380/USER',
            '/GammaGammaToEE_Elastic_Pt15_8TeV-lpair/Summer12-START53_V7C-v1/GEN-SIM'
           ]

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
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root'],
             'detail': True}],
         ['listFileArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/04FBE4D8-2DCF-E411-B827-02163E0124D5.root'],
             'detail': True}],
         ['listFileArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root'],
             'detail': True}],
         ['listFileArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/FA4E40C0-2DCF-E411-AD1A-02163E012186.root'],
             'detail': True}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'RAW'}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'RAW', 'detail': True}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'blah'}],
         ['listDatasets', {'primary_ds_name': 'blah', 'data_tier_name': 'RAW'}],
         ['listDatasets', {'dataset_access_type': '*', 'dataset': '/MET/Run2015B-05Aug2015-v1/DQMIO'}],
         ['listDatasets', {'dataset': '/NoBPTX/Run2016F-23Sep2016-v1/DQMIO', 'dataset_access_type': '*'}],
         ['listDatasets', {'dataset': '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM', 'dataset_access_type': '*'}],
         ['listDatasets', {'dataset': '/MinBias_TuneCP5_13TeV-pythia8_pilot/RunIIFall18MiniAOD-pilot_102X_upgrade2018_realistic_v11-v1/MINIAODSIM', 'dataset_access_type': '*'}],
         ['listDatasets', {'dataset': '/JetHT/Run2022B-PromptReco-v1/MINIAOD', 'dataset_access_type': '*'}],
         ['listFileLumiArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root']}],
         ['listFileLumiArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/04FBE4D8-2DCF-E411-B827-02163E0124D5.root']}],
         ['listFileLumiArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root']}],
         ['listFileLumiArray', {'logical_file_name': [
             '/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/FA4E40C0-2DCF-E411-AD1A-02163E012186.root']}],
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
         ['listFiles', {'dataset': '/Cosmics/ComissioningHI-v1/RAW', 'run_num': 180841,
                        'lumi_list': [8, 14, 5, 22, 10, 7, 13, 18, 23, 6, 16, 2, 4, 20, 3, 15, 9, 19, 17, 11, 12, 21, 1]}],
         ['listFiles', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'}],
         ['listFiles', {'block_name': '/Cosmics/ComissioningHI-PromptReco-v1/RECO#7020873e-0dcd-11e1-9b6c-003048caaace'}],
         ['listPrimaryDatasets', {'primary_ds_name': 'Jet*'}],
         ['listPrimaryDatasets', {'primary_ds_name': 'DoesntExist'}],
         ['listBlockParents',
          {'block_name': '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0'}],
         ['listBlockParents',
          {'block_name': '/Cosmics/Commissioning2015-v1/RAW#942d76fe-cf0e-11e4-afad-001e67ac06a0'}],

         # Exception throwing calls

         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas', 'detail': True}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#somethingelse'}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas'}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas'}],
         ['listBlocks', {'detail': True, 'dataset': '/HighPileUp/Run2011A-v1/RAWblah'}],
         ['listBlocks', {'detail': False, 'dataset': '/thisdoesntexist/ComissioningHI-v1/RAW'}],
         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas', 'detail': True,
           'dataset': '/HighPileUp/Run2011A-v1/RAW'}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#blah'}],
         ['listBlocks', {'detail': True, 'dataset': '/thisdoesntexist/ComissioningHI-v1/RAW'}],
         ['listDatasets', {'dataset_access_type': '*', 'dataset': '/HighPileUp/Run2011A-v1/RAWblah'}],
         ['listDatasets', {'dataset_access_type': '*', 'dataset': '/thisdoesntexist/ComissioningHI-v1/RAW'}],
         ['listDatasets', {'dataset_access_type': '*', 'dataset': '/MinimumBias/FAKE-Filter-v1/RECO'}],
         ['listDatasets', {'logical_file_name': '/store/data/ComissioningHI/Cosmics/RECO/PromptReco-v1/000/180/841/368B76AA-4F09-E111-82CB-BCAEC5329721.root'}],
         ['listParentDSTrio', {'dataset': '/Cosmics/ComissioningHI-PromptReco-v1/RECO'}],
         ]
