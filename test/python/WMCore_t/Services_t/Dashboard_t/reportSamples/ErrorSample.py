#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__ErrorSample__

Example of a report from a job that failed
Created on Fri Jun  8 13:22:11 2012

@author: dballest
"""

from WMCore.Configuration import ConfigSection
from WMCore.FwkJobReport.Report import Report

FrameworkJobReport = ConfigSection("FrameworkJobReport")
FrameworkJobReport.task = '/Run195530-PhotonHad-Run2012B-PromptReco-v1-PhotonHad/DataProcessing'
FrameworkJobReport.workload = 'Unknown'
FrameworkJobReport.section_('cmsRun1')
FrameworkJobReport.cmsRun1.status = 1
FrameworkJobReport.cmsRun1.counter = 1
FrameworkJobReport.cmsRun1.section_('errors')
FrameworkJobReport.cmsRun1.errors.section_('error0')
FrameworkJobReport.cmsRun1.errors.error0.type = 'CMSSWStepFailure'
FrameworkJobReport.cmsRun1.errors.error0.details = ''
FrameworkJobReport.cmsRun1.errors.error0.exitCode = 84
FrameworkJobReport.cmsRun1.errors.section_('error1')
FrameworkJobReport.cmsRun1.errors.error1.type = 'Fatal Exception'
FrameworkJobReport.cmsRun1.errors.error1.details = ''
FrameworkJobReport.cmsRun1.errors.error1.exitCode = '8020'
FrameworkJobReport.cmsRun1.errors.section_('error2')
FrameworkJobReport.cmsRun1.errors.error2.type = 'ErrorLoggingAddition'
FrameworkJobReport.cmsRun1.errors.error2.details = ''
FrameworkJobReport.cmsRun1.errors.errorCount = 3
FrameworkJobReport.cmsRun1.section_('logs')
FrameworkJobReport.cmsRun1.section_('parameters')
FrameworkJobReport.cmsRun1.parameters.GeneratorInfo = ''
FrameworkJobReport.cmsRun1.outputModules = []
FrameworkJobReport.cmsRun1.section_('site')
FrameworkJobReport.cmsRun1.section_('analysis')
FrameworkJobReport.cmsRun1.analysis.section_('files')
FrameworkJobReport.cmsRun1.analysis.files.fileCount = 0
FrameworkJobReport.cmsRun1.id = None
FrameworkJobReport.cmsRun1.section_('performance')
FrameworkJobReport.cmsRun1.section_('skipped')
FrameworkJobReport.cmsRun1.skipped.section_('files')
FrameworkJobReport.cmsRun1.skipped.files.section_('file0')
FrameworkJobReport.cmsRun1.skipped.files.file0.PhysicalFileName = 'rfio:/castor/grid.sinica.edu.tw/d0t1/cms/store/data/Run2012B/PhotonHad/RAW/v1/000/195/530/B66B7F04-04AF-E111-8D66-003048F024DE.root'
FrameworkJobReport.cmsRun1.skipped.files.file0.LogicalFileName = '/store/data/Run2012B/PhotonHad/RAW/v1/000/195/530/B66B7F04-04AF-E111-8D66-003048F024DE.root'
FrameworkJobReport.cmsRun1.skipped.files.fileCount = 1
FrameworkJobReport.cmsRun1.skipped.section_('events')
FrameworkJobReport.cmsRun1.startTime = 1339139482.98
FrameworkJobReport.cmsRun1.section_('input')
FrameworkJobReport.cmsRun1.input.section_('source')
FrameworkJobReport.cmsRun1.input.source.section_('files')
FrameworkJobReport.cmsRun1.input.source.files.section_('file0')
FrameworkJobReport.cmsRun1.input.source.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.input.source.files.file0.input_source_class = 'PoolSource'
FrameworkJobReport.cmsRun1.input.source.files.file0.input_type = 'primaryFiles'
FrameworkJobReport.cmsRun1.input.source.files.file0.lfn = '/store/data/Run2012B/PhotonHad/RECO/PromptReco-v1/000/195/530/FA34A15A-A0B0-E111-82C6-5404A63886B1.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.pfn = 'rfio:/castor/grid.sinica.edu.tw/d0t1/cms/store/data/Run2012B/PhotonHad/RECO/PromptReco-v1/000/195/530/FA34A15A-A0B0-E111-82C6-5404A63886B1.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.input.source.files.file0.module_label = 'source'
FrameworkJobReport.cmsRun1.input.source.files.file0.guid = 'FA34A15A-A0B0-E111-82C6-5404A63886B1'
FrameworkJobReport.cmsRun1.input.source.files.file0.events = 0
FrameworkJobReport.cmsRun1.input.source.files.fileCount = 1
FrameworkJobReport.cmsRun1.section_('output')
FrameworkJobReport.cmsRun1.section_('cleanup')
FrameworkJobReport.cmsRun1.cleanup.section_('unremoved')
FrameworkJobReport.cmsRun1.cleanup.section_('removed')
FrameworkJobReport.cmsRun1.cleanup.removed.fileCount = 0
FrameworkJobReport.siteName = 'T1_TW_ASGC'
FrameworkJobReport.completed = True
FrameworkJobReport.hostName = 'w-wn1154.grid.sinica.edu.tw'
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
FrameworkJobReport.logArch1.stopTime = 1339153091.34
FrameworkJobReport.logArch1.section_('site')
FrameworkJobReport.logArch1.section_('analysis')
FrameworkJobReport.logArch1.analysis.section_('files')
FrameworkJobReport.logArch1.analysis.files.fileCount = 0
FrameworkJobReport.logArch1.section_('performance')
FrameworkJobReport.logArch1.section_('skipped')
FrameworkJobReport.logArch1.skipped.section_('files')
FrameworkJobReport.logArch1.skipped.files.fileCount = 0
FrameworkJobReport.logArch1.skipped.section_('events')
FrameworkJobReport.logArch1.startTime = 1339153090.66
FrameworkJobReport.logArch1.section_('input')
FrameworkJobReport.logArch1.section_('output')
FrameworkJobReport.logArch1.output.section_('logArchive')
FrameworkJobReport.logArch1.output.logArchive.section_('files')
FrameworkJobReport.logArch1.output.logArchive.files.section_('file0')
FrameworkJobReport.logArch1.output.logArchive.files.file0.section_('runs')
FrameworkJobReport.logArch1.output.logArchive.files.file0.lfn = '/store/unmerged/logs/prod/2012/6/8/Run195530-PhotonHad-Run2012B-PromptReco-v1-PhotonHad/DataProcessing/0000/0/6aebe660-b137-11e1-a16b-003048c9c3fe-7-0-logArchive.tar.gz'
FrameworkJobReport.logArch1.output.logArchive.files.file0.pfn = '/castor/grid.sinica.edu.tw/d0t1/cms/store/unmerged/logs/prod/2012/6/8/Run195530-PhotonHad-Run2012B-PromptReco-v1-PhotonHad/DataProcessing/0000/0/6aebe660-b137-11e1-a16b-003048c9c3fe-7-0-logArchive.tar.gz'
FrameworkJobReport.logArch1.output.logArchive.files.file0.module_label = 'logArchive'
FrameworkJobReport.logArch1.output.logArchive.files.file0.location = 'srm2.grid.sinica.edu.tw'
FrameworkJobReport.logArch1.output.logArchive.files.file0.checksums = {'adler32': 'c33175bd', 'cksum': '2643592216', 'md5': '0377728362d501a56e04c2fdf8712968'}
FrameworkJobReport.logArch1.output.logArchive.files.file0.events = 0
FrameworkJobReport.logArch1.output.logArchive.files.file0.merged = False
FrameworkJobReport.logArch1.output.logArchive.files.file0.size = 0
FrameworkJobReport.logArch1.output.logArchive.files.fileCount = 1
FrameworkJobReport.logArch1.output.logArchive.section_('dataset')
FrameworkJobReport.logArch1.id = None
FrameworkJobReport.logArch1.counter = 2
FrameworkJobReport.WMAgentJobName = '6aebe660-b137-11e1-a16b-003048c9c3fe-7'
FrameworkJobReport.seName = 'srm2.grid.sinica.edu.tw'
FrameworkJobReport.WMAgentJobID = 62858
FrameworkJobReport.steps = ['cmsRun1', 'stageOut1', 'logArch1']
FrameworkJobReport.section_('stageOut1')
FrameworkJobReport.stageOut1.status = 1
FrameworkJobReport.stageOut1.section_('errors')
FrameworkJobReport.stageOut1.errors.section_('error0')
FrameworkJobReport.stageOut1.errors.error0.type = 'ReportManipulatingError'
FrameworkJobReport.stageOut1.errors.error0.details = 'Could not find report file for step stageOut1!'
FrameworkJobReport.stageOut1.errors.error0.exitCode = 99999
FrameworkJobReport.stageOut1.errors.errorCount = 1
FrameworkJobReport.stageOut1.section_('logs')
FrameworkJobReport.stageOut1.section_('parameters')
FrameworkJobReport.stageOut1.section_('output')
FrameworkJobReport.stageOut1.outputModules = []
FrameworkJobReport.stageOut1.section_('site')
FrameworkJobReport.stageOut1.section_('analysis')
FrameworkJobReport.stageOut1.analysis.section_('files')
FrameworkJobReport.stageOut1.analysis.files.fileCount = 0
FrameworkJobReport.stageOut1.section_('skipped')
FrameworkJobReport.stageOut1.skipped.section_('files')
FrameworkJobReport.stageOut1.skipped.files.fileCount = 0
FrameworkJobReport.stageOut1.skipped.section_('events')
FrameworkJobReport.stageOut1.section_('cleanup')
FrameworkJobReport.stageOut1.cleanup.section_('unremoved')
FrameworkJobReport.stageOut1.cleanup.section_('removed')
FrameworkJobReport.stageOut1.cleanup.removed.fileCount = 0
FrameworkJobReport.stageOut1.section_('input')
FrameworkJobReport.stageOut1.section_('performance')
FrameworkJobReport.stageOut1.id = None
FrameworkJobReport.stageOut1.counter = 3
FrameworkJobReport.ceName = 'w-wn1154.grid.sinica.edu.tw'

report = Report()
report.data = FrameworkJobReport
