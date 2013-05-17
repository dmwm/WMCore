#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__MergeSample__

Example of a report from a merge job
Created on Fri Jun  8 13:22:30 2012

@author: dballest
"""

from WMCore.Configuration import ConfigSection
from WMCore.FwkJobReport.Report import Report

FrameworkJobReport = ConfigSection("FrameworkJobReport")
FrameworkJobReport.task = '/Run195376-MuEG-Run2012B-PromptReco-v1-MuEG/DataProcessing/DataProcessingMergeSKIMStreamLogError'
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
FrameworkJobReport.cmsRun1.outputModules = ['Merged']
FrameworkJobReport.cmsRun1.stopTime = 1338808530.44
FrameworkJobReport.cmsRun1.section_('site')
FrameworkJobReport.cmsRun1.section_('analysis')
FrameworkJobReport.cmsRun1.analysis.section_('files')
FrameworkJobReport.cmsRun1.analysis.files.fileCount = 0
FrameworkJobReport.cmsRun1.section_('performance')
FrameworkJobReport.cmsRun1.performance.section_('memory')
FrameworkJobReport.cmsRun1.performance.memory.PeakValueRss = '0'
FrameworkJobReport.cmsRun1.performance.memory.PeakValueVsize = '0'
FrameworkJobReport.cmsRun1.performance.section_('storage')
FrameworkJobReport.cmsRun1.performance.storage.writeTotalMB = 1.42972
FrameworkJobReport.cmsRun1.performance.storage.readPercentageOps = 0.0236559139785
FrameworkJobReport.cmsRun1.performance.storage.readAveragekB = 66.5474327273
FrameworkJobReport.cmsRun1.performance.storage.readTotalMB = 1.42973
FrameworkJobReport.cmsRun1.performance.storage.readNumOps = 930.0
FrameworkJobReport.cmsRun1.performance.storage.readCachePercentageOps = 0.981720430108
FrameworkJobReport.cmsRun1.performance.storage.readMBSec = 0.0140973988838
FrameworkJobReport.cmsRun1.performance.storage.readMaxMSec = 36.706
FrameworkJobReport.cmsRun1.performance.storage.readTotalSecs = 0
FrameworkJobReport.cmsRun1.performance.storage.writeTotalSecs = 7861.25
FrameworkJobReport.cmsRun1.performance.section_('summaries')
FrameworkJobReport.cmsRun1.performance.section_('cpu')
FrameworkJobReport.cmsRun1.performance.cpu.TotalJobCPU = '2.06669'
FrameworkJobReport.cmsRun1.performance.cpu.AvgEventCPU = 'nan'
FrameworkJobReport.cmsRun1.performance.cpu.MaxEventTime = '0'
FrameworkJobReport.cmsRun1.performance.cpu.AvgEventTime = 'inf'
FrameworkJobReport.cmsRun1.performance.cpu.MinEventCPU = '0'
FrameworkJobReport.cmsRun1.performance.cpu.TotalEventCPU = '0'
FrameworkJobReport.cmsRun1.performance.cpu.TotalJobTime = '2.28855'
FrameworkJobReport.cmsRun1.performance.cpu.MinEventTime = '0'
FrameworkJobReport.cmsRun1.performance.cpu.MaxEventCPU = '0'
FrameworkJobReport.cmsRun1.section_('skipped')
FrameworkJobReport.cmsRun1.skipped.section_('files')
FrameworkJobReport.cmsRun1.skipped.files.fileCount = 0
FrameworkJobReport.cmsRun1.skipped.section_('events')
FrameworkJobReport.cmsRun1.startTime = 1338808520.0
FrameworkJobReport.cmsRun1.section_('input')
FrameworkJobReport.cmsRun1.input.section_('source')
FrameworkJobReport.cmsRun1.input.source.section_('files')
FrameworkJobReport.cmsRun1.input.source.files.section_('file0')
FrameworkJobReport.cmsRun1.input.source.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.input.source.files.file0.input_source_class = 'PoolSource'
FrameworkJobReport.cmsRun1.input.source.files.file0.input_type = 'primaryFiles'
FrameworkJobReport.cmsRun1.input.source.files.file0.lfn = '/store/unmerged/Run2012B/MuEG/RAW-RECO/LogError-PromptSkim-v1/000/195/376/0000/DCF6629B-7FAD-E111-8681-00266CFFCC7C.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.pfn = 'dcap://dcap.pic.es/pnfs/pic.es/data/cms/store/unmerged/Run2012B/MuEG/RAW-RECO/LogError-PromptSkim-v1/000/195/376/0000/DCF6629B-7FAD-E111-8681-00266CFFCC7C.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.input.source.files.file0.module_label = 'source'
FrameworkJobReport.cmsRun1.input.source.files.file0.guid = 'DCF6629B-7FAD-E111-8681-00266CFFCC7C'
FrameworkJobReport.cmsRun1.input.source.files.file0.events = 0
FrameworkJobReport.cmsRun1.input.source.files.fileCount = 1
FrameworkJobReport.cmsRun1.section_('output')
FrameworkJobReport.cmsRun1.output.section_('Merged')
FrameworkJobReport.cmsRun1.output.Merged.section_('files')
FrameworkJobReport.cmsRun1.output.Merged.files.section_('file0')
FrameworkJobReport.cmsRun1.output.Merged.files.file0.branch_hash = '11e0bdb184d9a2f4dfb472a644bc5c9e'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.user_dn = None
FrameworkJobReport.cmsRun1.output.Merged.files.file0.lfn = '/store/data/Run2012B/MuEG/RAW-RECO/LogError-PromptSkim-v1/000/195/376/0000/26BB8797-36AE-E111-915F-00266CFFCB80.root'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.dataset = {'applicationName': 'cmsRun', 'primaryDataset': 'MuEG', 'processedDataset': 'Run2012B-LogError-PromptSkim-v1', 'dataTier': 'RAW-RECO', 'applicationVersion': 'CMSSW_5_2_5_patch1'}
FrameworkJobReport.cmsRun1.output.Merged.files.file0.InputPFN = '/home/tmp/28014868.pbs03.pic.es/glide_n17244/execute/dir_19367/job/WMTaskSpace/cmsRun1/Merged.root'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.checksums = {'adler32': '9574cfd9', 'cksum': '4076992101'}
FrameworkJobReport.cmsRun1.output.Merged.files.file0.acquisitionEra = 'Run2012B'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.size = 1499084
FrameworkJobReport.cmsRun1.output.Merged.files.file0.guid = '26BB8797-36AE-E111-915F-00266CFFCB80'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.configURL = 'None;;None;;None'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.location = 'srmcms.pic.es'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.input = ['/store/unmerged/Run2012B/MuEG/RAW-RECO/LogError-PromptSkim-v1/000/195/376/0000/DCF6629B-7FAD-E111-8681-00266CFFCC7C.root']
FrameworkJobReport.cmsRun1.output.Merged.files.file0.validStatus = 'VALID'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.ouput_module_class = 'PoolOutputModule'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.globalTag = 'GR_P_V32::All'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.inputpfns = ['dcap://dcap.pic.es/pnfs/pic.es/data/cms/store/unmerged/Run2012B/MuEG/RAW-RECO/LogError-PromptSkim-v1/000/195/376/0000/DCF6629B-7FAD-E111-8681-00266CFFCC7C.root']
FrameworkJobReport.cmsRun1.output.Merged.files.file0.pfn = '/home/tmp/28014868.pbs03.pic.es/glide_n17244/execute/dir_19367/job/WMTaskSpace/cmsRun1/Merged.root'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.output.Merged.files.file0.module_label = 'Merged'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.async_dest = None
FrameworkJobReport.cmsRun1.output.Merged.files.file0.inputPath = None
FrameworkJobReport.cmsRun1.output.Merged.files.file0.StageOutCommand = 'srmv2-lcg'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.output.Merged.files.file0.OutputPFN = 'srm://srmcms.pic.es:8443/srm/managerv2?SFN=/pnfs/pic.es/data/cms/store/data/Run2012B/MuEG/RAW-RECO/LogError-PromptSkim-v1/000/195/376/0000/26BB8797-36AE-E111-915F-00266CFFCB80.root'
FrameworkJobReport.cmsRun1.output.Merged.files.file0.user_vogroup = ''
FrameworkJobReport.cmsRun1.output.Merged.files.file0.user_vorole = ''
FrameworkJobReport.cmsRun1.output.Merged.files.file0.events = 0
FrameworkJobReport.cmsRun1.output.Merged.files.file0.processingVer = 1
FrameworkJobReport.cmsRun1.output.Merged.files.fileCount = 1
FrameworkJobReport.cmsRun1.output.Merged.section_('dataset')
FrameworkJobReport.cmsRun1.id = None
FrameworkJobReport.siteName = 'T1_ES_PIC'
FrameworkJobReport.completed = True
FrameworkJobReport.hostName = 'td636.pic.es'
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
FrameworkJobReport.logArch1.stopTime = 1338808560.42
FrameworkJobReport.logArch1.section_('site')
FrameworkJobReport.logArch1.section_('analysis')
FrameworkJobReport.logArch1.analysis.section_('files')
FrameworkJobReport.logArch1.analysis.files.fileCount = 0
FrameworkJobReport.logArch1.section_('performance')
FrameworkJobReport.logArch1.section_('skipped')
FrameworkJobReport.logArch1.skipped.section_('files')
FrameworkJobReport.logArch1.skipped.files.fileCount = 0
FrameworkJobReport.logArch1.skipped.section_('events')
FrameworkJobReport.logArch1.startTime = 1338808536.04
FrameworkJobReport.logArch1.section_('input')
FrameworkJobReport.logArch1.section_('output')
FrameworkJobReport.logArch1.output.section_('logArchive')
FrameworkJobReport.logArch1.output.logArchive.section_('files')
FrameworkJobReport.logArch1.output.logArchive.files.section_('file0')
FrameworkJobReport.logArch1.output.logArchive.files.file0.section_('runs')
FrameworkJobReport.logArch1.output.logArchive.files.file0.lfn = '/store/unmerged/logs/prod/2012/6/4/Run195376-MuEG-Run2012B-PromptReco-v1-MuEG/DataProcessing/DataProcessingMergeSKIMStreamLogError/0000/0/d0080012-ae33-11e1-a16b-003048c9c3fe-0-0-logArchive.tar.gz'
FrameworkJobReport.logArch1.output.logArchive.files.file0.pfn = 'srm://srmcms.pic.es:8443/srm/managerv2?SFN=/pnfs/pic.es/data/cms/store/unmerged/logs/prod/2012/6/4/Run195376-MuEG-Run2012B-PromptReco-v1-MuEG/DataProcessing/DataProcessingMergeSKIMStreamLogError/0000/0/d0080012-ae33-11e1-a16b-003048c9c3fe-0-0-logArchive.tar.gz'
FrameworkJobReport.logArch1.output.logArchive.files.file0.module_label = 'logArchive'
FrameworkJobReport.logArch1.output.logArchive.files.file0.location = 'srmcms.pic.es'
FrameworkJobReport.logArch1.output.logArchive.files.file0.checksums = {'adler32': '47ae1f48', 'cksum': '1500280488', 'md5': 'd47b198efa54e0432c346169ae36bfb5'}
FrameworkJobReport.logArch1.output.logArchive.files.file0.events = 0
FrameworkJobReport.logArch1.output.logArchive.files.file0.merged = False
FrameworkJobReport.logArch1.output.logArchive.files.file0.size = 0
FrameworkJobReport.logArch1.output.logArchive.files.fileCount = 1
FrameworkJobReport.logArch1.output.logArchive.section_('dataset')
FrameworkJobReport.logArch1.id = None
FrameworkJobReport.logArch1.counter = 2
FrameworkJobReport.WMAgentJobName = 'd0080012-ae33-11e1-a16b-003048c9c3fe-0'
FrameworkJobReport.seName = 'srmcms.pic.es'
FrameworkJobReport.WMAgentJobID = 43441
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
FrameworkJobReport.stageOut1.stopTime = 1338808536.02
FrameworkJobReport.stageOut1.section_('site')
FrameworkJobReport.stageOut1.section_('analysis')
FrameworkJobReport.stageOut1.analysis.section_('files')
FrameworkJobReport.stageOut1.analysis.files.fileCount = 0
FrameworkJobReport.stageOut1.section_('performance')
FrameworkJobReport.stageOut1.section_('skipped')
FrameworkJobReport.stageOut1.skipped.section_('files')
FrameworkJobReport.stageOut1.skipped.files.fileCount = 0
FrameworkJobReport.stageOut1.skipped.section_('events')
FrameworkJobReport.stageOut1.startTime = 1338808530.49
FrameworkJobReport.stageOut1.section_('input')
FrameworkJobReport.stageOut1.section_('output')
FrameworkJobReport.stageOut1.id = None
FrameworkJobReport.stageOut1.counter = 3
FrameworkJobReport.ceName = 'td636.pic.es'

report = Report()
report.data = FrameworkJobReport
