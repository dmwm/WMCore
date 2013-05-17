#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__ProcessingSample__

Example of a report from a processing job with multiple output modules
Created on Fri Jun  8 11:27:53 2012

@author: dballest
"""
from WMCore.Configuration import ConfigSection
from WMCore.FwkJobReport.Report import Report

FrameworkJobReport = ConfigSection("FrameworkJobReport")
FrameworkJobReport.task = '/Run195529-SingleElectron-Run2012B-PromptReco-v1-SingleElectron/DataProcessing'
FrameworkJobReport.workload = 'Unknown'
FrameworkJobReport.section_('cmsRun1')
FrameworkJobReport.cmsRun1.status = 0
FrameworkJobReport.cmsRun1.counter = 1
FrameworkJobReport.cmsRun1.section_('cleanup')
FrameworkJobReport.cmsRun1.cleanup.section_('unremoved')
FrameworkJobReport.cmsRun1.cleanup.section_('removed')
FrameworkJobReport.cmsRun1.cleanup.removed.fileCount = 0
FrameworkJobReport.cmsRun1.section_('errors')
FrameworkJobReport.cmsRun1.section_('logs')
FrameworkJobReport.cmsRun1.section_('parameters')
FrameworkJobReport.cmsRun1.parameters.GeneratorInfo = ''
FrameworkJobReport.cmsRun1.parameters.ReadBranches = ''
FrameworkJobReport.cmsRun1.outputModules = ['SKIMStreamDiTau', 'SKIMStreamHighMET', 'SKIMStreamLogError', 'SKIMStreamLogErrorMonitor', 'SKIMStreamTOPElePlusJets', 'SKIMStreamWElectron']
FrameworkJobReport.cmsRun1.stopTime = 1339094634.7
FrameworkJobReport.cmsRun1.section_('site')
FrameworkJobReport.cmsRun1.section_('analysis')
FrameworkJobReport.cmsRun1.analysis.section_('files')
FrameworkJobReport.cmsRun1.analysis.files.fileCount = 0
FrameworkJobReport.cmsRun1.section_('performance')
FrameworkJobReport.cmsRun1.performance.section_('memory')
FrameworkJobReport.cmsRun1.performance.memory.PeakValueRss = '891.617'
FrameworkJobReport.cmsRun1.performance.memory.PeakValueVsize = '1140.02'
FrameworkJobReport.cmsRun1.performance.section_('storage')
FrameworkJobReport.cmsRun1.performance.storage.writeTotalMB = 2024.23
FrameworkJobReport.cmsRun1.performance.storage.readPercentageOps = 0.00771119755586
FrameworkJobReport.cmsRun1.performance.storage.readAveragekB = 129390.866142
FrameworkJobReport.cmsRun1.performance.storage.readTotalMB = 16047.5
FrameworkJobReport.cmsRun1.performance.storage.readNumOps = 1351152.0
FrameworkJobReport.cmsRun1.performance.storage.readCachePercentageOps = 0.995779157341
FrameworkJobReport.cmsRun1.performance.storage.readMBSec = 0.041078234871
FrameworkJobReport.cmsRun1.performance.storage.readMaxMSec = 9554.83
FrameworkJobReport.cmsRun1.performance.storage.readTotalSecs = 0
FrameworkJobReport.cmsRun1.performance.storage.writeTotalSecs = 21823100.0
FrameworkJobReport.cmsRun1.performance.section_('summaries')
FrameworkJobReport.cmsRun1.performance.section_('cpu')
FrameworkJobReport.cmsRun1.performance.cpu.TotalJobCPU = '998.07'
FrameworkJobReport.cmsRun1.performance.cpu.AvgEventCPU = '0.0966836'
FrameworkJobReport.cmsRun1.performance.cpu.MaxEventTime = '3.32538'
FrameworkJobReport.cmsRun1.performance.cpu.AvgEventTime = '0.158431'
FrameworkJobReport.cmsRun1.performance.cpu.MinEventCPU = '0.002'
FrameworkJobReport.cmsRun1.performance.cpu.TotalEventCPU = '879.434'
FrameworkJobReport.cmsRun1.performance.cpu.TotalJobTime = '1441.09'
FrameworkJobReport.cmsRun1.performance.cpu.MinEventTime = '0.00538898'
FrameworkJobReport.cmsRun1.performance.cpu.MaxEventCPU = '1.87772'
FrameworkJobReport.cmsRun1.section_('skipped')
FrameworkJobReport.cmsRun1.skipped.section_('files')
FrameworkJobReport.cmsRun1.skipped.files.fileCount = 0
FrameworkJobReport.cmsRun1.skipped.section_('events')
FrameworkJobReport.cmsRun1.startTime = 1339092709.9
FrameworkJobReport.cmsRun1.section_('input')
FrameworkJobReport.cmsRun1.input.section_('source')
FrameworkJobReport.cmsRun1.input.source.section_('files')
FrameworkJobReport.cmsRun1.input.source.files.section_('file1')
FrameworkJobReport.cmsRun1.input.source.files.file1.section_('runs')
FrameworkJobReport.cmsRun1.input.source.files.file1.input_source_class = 'PoolSource'
FrameworkJobReport.cmsRun1.input.source.files.file1.input_type = 'secondaryFiles'
FrameworkJobReport.cmsRun1.input.source.files.file1.lfn = '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root'
FrameworkJobReport.cmsRun1.input.source.files.file1.pfn = 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root'
FrameworkJobReport.cmsRun1.input.source.files.file1.catalog = ''
FrameworkJobReport.cmsRun1.input.source.files.file1.module_label = 'source'
FrameworkJobReport.cmsRun1.input.source.files.file1.guid = '84F460A0-BCAE-E111-B482-001D09F28D54'
FrameworkJobReport.cmsRun1.input.source.files.file1.events = 9096
FrameworkJobReport.cmsRun1.input.source.files.section_('file0')
FrameworkJobReport.cmsRun1.input.source.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.input.source.files.file0.input_source_class = 'PoolSource'
FrameworkJobReport.cmsRun1.input.source.files.file0.input_type = 'primaryFiles'
FrameworkJobReport.cmsRun1.input.source.files.file0.lfn = '/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.pfn = 'dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.input.source.files.file0.module_label = 'source'
FrameworkJobReport.cmsRun1.input.source.files.file0.guid = '8A60F151-3EB0-E111-AD30-001D09F26509'
FrameworkJobReport.cmsRun1.input.source.files.file0.events = 9096
FrameworkJobReport.cmsRun1.input.source.files.fileCount = 2
FrameworkJobReport.cmsRun1.section_('output')
FrameworkJobReport.cmsRun1.output.section_('SKIMStreamWElectron')
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.section_('files')
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.section_('file0')
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.branch_hash = 'ef57d4bffd9b68b3181c34702357743f'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.lfn = '/store/data/Run2012B/SingleElectron/USER/WElectron-PromptSkim-v1/000/195/529/0000/6428DF41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'SingleElectron', 'processedDataset': 'Run2012B-WElectron-PromptSkim-v1', 'dataTier': 'USER', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.InputPFN = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamWElectron.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.checksums = {'adler32': '57dbc61b', 'cksum': '4144407125'}
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.size = 652370707
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.guid = '6428DF41-CDB0-E111-B71D-001A92971BD6'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.configURL = 'bogus;;bogus;;bogus'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.input = ['/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.events = 1603
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.merged = True
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.globalTag = 'GR_P_V35::All'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.pfn = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamWElectron.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.module_label = 'SKIMStreamWElectron'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.inputPath = '/SingleElectron/Run2012B-PromptReco-v1/RECO'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.StageOutCommand = 'stageout-fnal'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.inputpfns = ['dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.OutputPFN = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/Run2012B/SingleElectron/USER/WElectron-PromptSkim-v1/000/195/529/0000/6428DF41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.user_vogroup = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.user_vorole = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamWElectron.section_('dataset')
FrameworkJobReport.cmsRun1.output.section_('SKIMStreamLogErrorMonitor')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.section_('files')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.section_('file0')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.branch_hash = 'd0a84c04b883caa06b45342bb7c79692'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.lfn = '/store/unmerged/Run2012B/SingleElectron/USER/LogErrorMonitor-PromptSkim-v1/000/195/529/0000/F03FD741-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'SingleElectron', 'processedDataset': 'Run2012B-LogErrorMonitor-PromptSkim-v1', 'dataTier': 'USER', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.InputPFN = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamLogErrorMonitor.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.checksums = {'adler32': 'd74abdab', 'cksum': '2561799877'}
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.size = 1326010
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.guid = 'F03FD741-CDB0-E111-B71D-001A92971BD6'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.configURL = 'bogus;;bogus;;bogus'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.input = ['/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.globalTag = 'GR_P_V35::All'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.inputpfns = ['dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.pfn = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamLogErrorMonitor.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.module_label = 'SKIMStreamLogErrorMonitor'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.inputPath = '/SingleElectron/Run2012B-PromptReco-v1/RECO'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.StageOutCommand = 'stageout-fnal'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.OutputPFN = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/unmerged/Run2012B/SingleElectron/USER/LogErrorMonitor-PromptSkim-v1/000/195/529/0000/F03FD741-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.user_vogroup = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.user_vorole = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.events = 137
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamLogErrorMonitor.section_('dataset')
FrameworkJobReport.cmsRun1.output.section_('SKIMStreamLogError')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.section_('files')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.section_('file0')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.branch_hash = '11e0bdb184d9a2f4dfb472a644bc5c9e'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.lfn = '/store/unmerged/Run2012B/SingleElectron/RAW-RECO/LogError-PromptSkim-v1/000/195/529/0000/F8F8D441-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'SingleElectron', 'processedDataset': 'Run2012B-LogError-PromptSkim-v1', 'dataTier': 'RAW-RECO', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.InputPFN = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamLogError.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.checksums = {'adler32': 'ed7285b7', 'cksum': '3708016856'}
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.size = 132220254
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.guid = 'F8F8D441-CDB0-E111-B71D-001A92971BD6'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.configURL = 'bogus;;bogus;;bogus'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.input = ['/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.globalTag = 'GR_P_V35::All'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.inputpfns = ['dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.pfn = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamLogError.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.module_label = 'SKIMStreamLogError'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.inputPath = '/SingleElectron/Run2012B-PromptReco-v1/RECO'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.StageOutCommand = 'stageout-fnal'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.OutputPFN = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/unmerged/Run2012B/SingleElectron/RAW-RECO/LogError-PromptSkim-v1/000/195/529/0000/F8F8D441-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.user_vogroup = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.user_vorole = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.events = 66
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamLogError.section_('dataset')
FrameworkJobReport.cmsRun1.output.section_('SKIMStreamTOPElePlusJets')
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.section_('files')
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.section_('file0')
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.branch_hash = 'b89c9efcd0d19ec6dec21102efb33409'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.lfn = '/store/data/Run2012B/SingleElectron/AOD/TOPElePlusJets-PromptSkim-v1/000/195/529/0000/24C0DC41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'SingleElectron', 'processedDataset': 'Run2012B-TOPElePlusJets-PromptSkim-v1', 'dataTier': 'AOD', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.InputPFN = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamTOPElePlusJets.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.checksums = {'adler32': '99a9e4f6', 'cksum': '434088319'}
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.size = 978843596
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.guid = '24C0DC41-CDB0-E111-B71D-001A92971BD6'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.configURL = 'bogus;;bogus;;bogus'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.input = ['/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.events = 2320
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.merged = True
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.globalTag = 'GR_P_V35::All'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.pfn = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamTOPElePlusJets.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.module_label = 'SKIMStreamTOPElePlusJets'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.inputPath = '/SingleElectron/Run2012B-PromptReco-v1/RECO'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.StageOutCommand = 'stageout-fnal'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.inputpfns = ['dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.OutputPFN = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/data/Run2012B/SingleElectron/AOD/TOPElePlusJets-PromptSkim-v1/000/195/529/0000/24C0DC41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.user_vogroup = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.user_vorole = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamTOPElePlusJets.section_('dataset')
FrameworkJobReport.cmsRun1.output.section_('SKIMStreamHighMET')
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.section_('files')
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.section_('file0')
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.branch_hash = '11e0bdb184d9a2f4dfb472a644bc5c9e'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.lfn = '/store/unmerged/Run2012B/SingleElectron/RAW-RECO/HighMET-PromptSkim-v1/000/195/529/0000/5E2ECB41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'SingleElectron', 'processedDataset': 'Run2012B-HighMET-PromptSkim-v1', 'dataTier': 'RAW-RECO', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.InputPFN = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamHighMET.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.checksums = {'adler32': '690af315', 'cksum': '3016754516'}
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.size = 17881085
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.guid = '5E2ECB41-CDB0-E111-B71D-001A92971BD6'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.configURL = 'bogus;;bogus;;bogus'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.input = ['/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.globalTag = 'GR_P_V35::All'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.inputpfns = ['dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.pfn = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamHighMET.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.module_label = 'SKIMStreamHighMET'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.inputPath = '/SingleElectron/Run2012B-PromptReco-v1/RECO'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.StageOutCommand = 'stageout-fnal'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.OutputPFN = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/unmerged/Run2012B/SingleElectron/RAW-RECO/HighMET-PromptSkim-v1/000/195/529/0000/5E2ECB41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.user_vogroup = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.user_vorole = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.events = 8
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamHighMET.section_('dataset')
FrameworkJobReport.cmsRun1.output.section_('SKIMStreamDiTau')
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.section_('files')
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.section_('file0')
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.branch_hash = '11e0bdb184d9a2f4dfb472a644bc5c9e'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.lfn = '/store/unmerged/Run2012B/SingleElectron/RAW-RECO/DiTau-PromptSkim-v1/000/195/529/0000/30ADBB41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'SingleElectron', 'processedDataset': 'Run2012B-DiTau-PromptSkim-v1', 'dataTier': 'RAW-RECO', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.InputPFN = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamDiTau.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.checksums = {'adler32': '7d553e81', 'cksum': '463952083'}
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.size = 339912126
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.guid = '30ADBB41-CDB0-E111-B71D-001A92971BD6'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.configURL = 'bogus;;bogus;;bogus'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.input = ['/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', '/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.globalTag = 'GR_P_V35::All'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.inputpfns = ['dcap://cmsdca1.fnal.gov:24141/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RECO/PromptReco-v1/000/195/529/8A60F151-3EB0-E111-AD30-001D09F26509.root', 'dcap://cmsdca.fnal.gov:24137/pnfs/fnal.gov/usr/cms/WAX/11/store/data/Run2012B/SingleElectron/RAW/v1/000/195/529/84F460A0-BCAE-E111-B482-001D09F28D54.root']
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.pfn = '/storage/local/data1/condor/execute/dir_31310/glide_F31718/execute/dir_5430/job/WMTaskSpace/cmsRun1/SKIMStreamDiTau.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.module_label = 'SKIMStreamDiTau'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.inputPath = '/SingleElectron/Run2012B-PromptReco-v1/RECO'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.StageOutCommand = 'stageout-fnal'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.OutputPFN = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/unmerged/Run2012B/SingleElectron/RAW-RECO/DiTau-PromptSkim-v1/000/195/529/0000/30ADBB41-CDB0-E111-B71D-001A92971BD6.root'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.user_vogroup = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.user_vorole = 'DEFAULT'
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.events = 192
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.SKIMStreamDiTau.section_('dataset')
FrameworkJobReport.cmsRun1.id = None
FrameworkJobReport.siteName = 'T1_US_FNAL'
FrameworkJobReport.completed = True
FrameworkJobReport.hostName = 'cmswn970.fnal.gov'
FrameworkJobReport.section_('logArch1')
FrameworkJobReport.logArch1.status = 0
FrameworkJobReport.logArch1.section_('cleanup')
FrameworkJobReport.logArch1.cleanup.section_('unremoved')
FrameworkJobReport.logArch1.cleanup.section_('removed')
FrameworkJobReport.logArch1.cleanup.removed.fileCount = 0
FrameworkJobReport.logArch1.section_('errors')
FrameworkJobReport.logArch1.section_('logs')
FrameworkJobReport.logArch1.section_('parameters')
FrameworkJobReport.logArch1.outputModules = ['logArchive']
FrameworkJobReport.logArch1.stopTime = 1339094724.0
FrameworkJobReport.logArch1.section_('site')
FrameworkJobReport.logArch1.section_('analysis')
FrameworkJobReport.logArch1.analysis.section_('files')
FrameworkJobReport.logArch1.analysis.files.fileCount = 0
FrameworkJobReport.logArch1.section_('performance')
FrameworkJobReport.logArch1.section_('skipped')
FrameworkJobReport.logArch1.skipped.section_('files')
FrameworkJobReport.logArch1.skipped.files.fileCount = 0
FrameworkJobReport.logArch1.skipped.section_('events')
FrameworkJobReport.logArch1.startTime = 1339094723.32
FrameworkJobReport.logArch1.section_('input')
FrameworkJobReport.logArch1.section_('output')
FrameworkJobReport.logArch1.output.section_('logArchive')
FrameworkJobReport.logArch1.output.logArchive.section_('files')
FrameworkJobReport.logArch1.output.logArchive.files.section_('file0')
FrameworkJobReport.logArch1.output.logArchive.files.file0.section_('runs')
FrameworkJobReport.logArch1.output.logArchive.files.file0.lfn = '/store/unmerged/logs/prod/2012/6/7/Run195529-SingleElectron-Run2012B-PromptReco-v1-SingleElectron/DataProcessing/0000/0/f44abb00-b0cb-11e1-a16b-003048c9c3fe-0-0-logArchive.tar.gz'
FrameworkJobReport.logArch1.output.logArchive.files.file0.pfn = 'srm://cmssrm.fnal.gov:8443/srm/managerv2?SFN=/11/store/unmerged/logs/prod/2012/6/7/Run195529-SingleElectron-Run2012B-PromptReco-v1-SingleElectron/DataProcessing/0000/0/f44abb00-b0cb-11e1-a16b-003048c9c3fe-0-0-logArchive.tar.gz'
FrameworkJobReport.logArch1.output.logArchive.files.file0.module_label = 'logArchive'
FrameworkJobReport.logArch1.output.logArchive.files.file0.location = 'cmssrm.fnal.gov'
FrameworkJobReport.logArch1.output.logArchive.files.file0.checksums = {'adler32': '22c4fa2a', 'cksum': '3521757287', 'md5': '82832706d91b7d22a3512a3e820e39c4'}
FrameworkJobReport.logArch1.output.logArchive.files.file0.events = 0
FrameworkJobReport.logArch1.output.logArchive.files.file0.merged = False
FrameworkJobReport.logArch1.output.logArchive.files.file0.size = 0
FrameworkJobReport.logArch1.output.logArchive.files.fileCount = 1
FrameworkJobReport.logArch1.output.logArchive.section_('dataset')
FrameworkJobReport.logArch1.id = None
FrameworkJobReport.logArch1.counter = 2
FrameworkJobReport.WMAgentJobName = 'f44abb00-b0cb-11e1-a16b-003048c9c3fe-0'
FrameworkJobReport.seName = 'cmssrm.fnal.gov'
FrameworkJobReport.WMAgentJobID = 62480
FrameworkJobReport.steps = ['cmsRun1', 'stageOut1', 'logArch1']
FrameworkJobReport.section_('stageOut1')
FrameworkJobReport.stageOut1.status = 0
FrameworkJobReport.stageOut1.section_('cleanup')
FrameworkJobReport.stageOut1.cleanup.section_('unremoved')
FrameworkJobReport.stageOut1.cleanup.section_('removed')
FrameworkJobReport.stageOut1.cleanup.removed.fileCount = 0
FrameworkJobReport.stageOut1.section_('errors')
FrameworkJobReport.stageOut1.section_('logs')
FrameworkJobReport.stageOut1.section_('parameters')
FrameworkJobReport.stageOut1.outputModules = []
FrameworkJobReport.stageOut1.stopTime = 1339094723.15
FrameworkJobReport.stageOut1.section_('site')
FrameworkJobReport.stageOut1.section_('analysis')
FrameworkJobReport.stageOut1.analysis.section_('files')
FrameworkJobReport.stageOut1.analysis.files.fileCount = 0
FrameworkJobReport.stageOut1.section_('performance')
FrameworkJobReport.stageOut1.section_('skipped')
FrameworkJobReport.stageOut1.skipped.section_('files')
FrameworkJobReport.stageOut1.skipped.files.fileCount = 0
FrameworkJobReport.stageOut1.skipped.section_('events')
FrameworkJobReport.stageOut1.startTime = 1339094635.29
FrameworkJobReport.stageOut1.section_('input')
FrameworkJobReport.stageOut1.section_('output')
FrameworkJobReport.stageOut1.id = None
FrameworkJobReport.stageOut1.counter = 3
FrameworkJobReport.ceName = 'cmsosgce.fnal.gov'

report = Report()
report.data = FrameworkJobReport
