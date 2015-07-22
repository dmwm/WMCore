#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
__FallbackSample__

Example of a report from a job that had xrootd fallback reads

"""

from WMCore.Configuration import ConfigSection
from WMCore.FwkJobReport.Report import Report

FrameworkJobReport = ConfigSection("FrameworkJobReport")
FrameworkJobReport.task = '/Run195530-PhotonHad-Run2012B-PromptReco-v1-PhotonHad/DataProcessing'
FrameworkJobReport.workload = 'Unknown'
FrameworkJobReport.section_('cmsRun1')
FrameworkJobReport.cmsRun1.status = 1
FrameworkJobReport.cmsRun1.section_('cleanup')
FrameworkJobReport.cmsRun1.cleanup.section_('unremoved')
FrameworkJobReport.cmsRun1.cleanup.section_('removed')
FrameworkJobReport.cmsRun1.cleanup.removed.fileCount = 0
FrameworkJobReport.cmsRun1.section_('errors')
FrameworkJobReport.cmsRun1.section_('logs')
FrameworkJobReport.cmsRun1.section_('parameters')
FrameworkJobReport.cmsRun1.parameters.ReadBranches = ''
FrameworkJobReport.cmsRun1.outputModules = []
FrameworkJobReport.cmsRun1.section_('site')
FrameworkJobReport.cmsRun1.section_('analysis')
FrameworkJobReport.cmsRun1.analysis.section_('files')
FrameworkJobReport.cmsRun1.analysis.files.fileCount = 0
FrameworkJobReport.cmsRun1.section_('performance')
FrameworkJobReport.cmsRun1.performance.section_('memory')
FrameworkJobReport.cmsRun1.performance.section_('storage')
FrameworkJobReport.cmsRun1.performance.storage.writeTotalMB = 0
FrameworkJobReport.cmsRun1.performance.storage.readPercentageOps = 2.38888888889
FrameworkJobReport.cmsRun1.performance.storage.readAveragekB = 7421.23591442
FrameworkJobReport.cmsRun1.performance.storage.readTotalMB = 311.63393
FrameworkJobReport.cmsRun1.performance.storage.readNumOps = 18.0
FrameworkJobReport.cmsRun1.performance.storage.readCachePercentageOps = 0.0
FrameworkJobReport.cmsRun1.performance.storage.readMBSec = 0.0135009760282
FrameworkJobReport.cmsRun1.performance.storage.readMaxMSec = 3325.76
FrameworkJobReport.cmsRun1.performance.storage.readTotalSecs = 0
FrameworkJobReport.cmsRun1.performance.storage.writeTotalSecs = 0
FrameworkJobReport.cmsRun1.performance.section_('summaries')
FrameworkJobReport.cmsRun1.performance.section_('cpu')
FrameworkJobReport.cmsRun1.section_('skipped')
FrameworkJobReport.cmsRun1.skipped.section_('files')
FrameworkJobReport.cmsRun1.skipped.files.fileCount = 0
FrameworkJobReport.cmsRun1.skipped.section_('events')
FrameworkJobReport.cmsRun1.section_('input')
FrameworkJobReport.cmsRun1.input.section_('source')
FrameworkJobReport.cmsRun1.input.source.section_('files')
FrameworkJobReport.cmsRun1.input.source.files.section_('file0')
FrameworkJobReport.cmsRun1.input.source.files.file0.section_('runs')
FrameworkJobReport.cmsRun1.input.source.files.file0.input_source_class = 'PoolSource'
FrameworkJobReport.cmsRun1.input.source.files.file0.input_type = 'primaryFiles'
FrameworkJobReport.cmsRun1.input.source.files.file0.lfn = '/store/data/Run2012D/SingleElectron/AOD/PromptReco-v1/000/207/279/D43A5B72-1831-E211-895D-001D09F24763.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.pfn = 'root://xrootd.unl.edu//store/data/Run2012D/SingleElectron/AOD/PromptReco-v1/000/207/279/D43A5B72-1831-E211-895D-001D09F24763.root'
FrameworkJobReport.cmsRun1.input.source.files.file0.catalog = ''
FrameworkJobReport.cmsRun1.input.source.files.file0.module_label = 'source'
FrameworkJobReport.cmsRun1.input.source.files.file0.guid = 'D43A5B72-1831-E211-895D-001D09F24763'
FrameworkJobReport.cmsRun1.input.source.files.file0.events = 1215
FrameworkJobReport.cmsRun1.input.source.files.fileCount = 1
FrameworkJobReport.cmsRun1.section_('output')
FrameworkJobReport.cmsRun1.section_('fallback')
FrameworkJobReport.cmsRun1.fallback.section_('files')
FrameworkJobReport.cmsRun1.fallback.files.section_('file0')
FrameworkJobReport.cmsRun1.fallback.files.file0.PhysicalFileName = 'root://xrootd.unl.edu//store/data/Run2012D/SingleElectron/AOD/PromptReco-v1/000/207/279/D43A5B72-1831-E211-895D-001D09F24763.root'
FrameworkJobReport.cmsRun1.fallback.files.file0.LogicalFileName = '/store/data/Run2012D/SingleElectron/AOD/PromptReco-v1/000/207/279/D43A5B72-1831-E211-895D-001D09F24763.root'
FrameworkJobReport.cmsRun1.fallback.files.fileCount = 1
FrameworkJobReport.cmsRun1.id = None
FrameworkJobReport.workload = 'Unknown'
FrameworkJobReport.steps = ['cmsRun1']

report = Report()
report.data = FrameworkJobReport

