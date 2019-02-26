#!/usr/bin/env python

from __future__ import (division, print_function)

endpoint = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'

# Datasets to get all blocks, array, and lumis from
datasets = ['/HighPileUp/Run2011A-v1/RAW', '/MinimumBias/ComissioningHI-v1/RAW', '/Cosmics/ComissioningHI-v1/RAW',
            '/Cosmics/ComissioningHI-PromptReco-v1/RECO',
            '/SingleElectron/StoreResults-Run2011A-WElectron-PromptSkim-v4-ALCARECO-NOLC-36cfce5a1d3f3ab4df5bd2aa0a4fa380/USER',
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
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root'],
             'detail': True}],
         ['listFileArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/04FBE4D8-2DCF-E411-B827-02163E0124D5.root'],
             'detail': True}],
         ['listFileArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root'],
             'detail': True}],
         ['listFileArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/FA4E40C0-2DCF-E411-AD1A-02163E012186.root'],
             'detail': True}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'RAW'}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'RAW', 'detail': True}],
         ['listDatasets', {'primary_ds_name': 'Jet', 'data_tier_name': 'blah'}],
         ['listDatasets', {'primary_ds_name': 'blah', 'data_tier_name': 'RAW'}],
         ['listDatasets', {'dataset_access_type': '*', 'dataset': '/MET/Run2015B-05Aug2015-v1/DQMIO'}],
         ['listDatasets', {'dataset': '/NoBPTX/Run2016F-23Sep2016-v1/DQMIO', 'dataset_access_type': '*'}],
         ['listDatasets', {'dataset': '/BprimeJetToBZ_M800GeV_Tune4C_13TeV-madgraph-tauola/Fall13-POSTLS162_V1-v1/GEN-SIM', 'dataset_access_type': '*'}],
         ['listFileLumiArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root']}],
         ['listFileLumiArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/04FBE4D8-2DCF-E411-B827-02163E0124D5.root']}],
         ['listFileLumiArray', {'logical_file_name': [
             u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/1043E89F-2DCF-E411-9CAE-02163E013751.root']}],
         ['listFileLumiArray', {'logical_file_name': [
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
         ['listBlockParents',
          {'block_name': '/Cosmics/Commissioning2015-v1/RAW#942d76fe-cf0e-11e4-afad-001e67ac06a0'}],

         # Exception throwing calls

         ['listBlocks',
          {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas', 'detail': True}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#somethingelse'}],
         ['listBlocks', {'block_name': '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas'}],
         ['listBlocks', {'block_name': u'/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaaceasas'}],
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

         ]
