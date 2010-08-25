#!/usr/bin/env python
# encoding: utf-8
"""
PSet.py

Really dumb read/write configuration for testing purposes

Created by Dave Evans on 2010-03-27.
Copyright (c) 2010 Fermilab. All rights reserved.
"""


import FWCore.ParameterSet.Config as cms



process = cms.Process("MyAnalysis")

 
process.source = cms.Source("PoolSource")
process.source.fileNames = cms.untracked(cms.vstring())
 
process.writeData1 = cms.OutputModule("PoolOutputModule")
process.writeData1.fileName = cms.untracked(cms.string("writeData1.root"))
process.writeData1.logicalFileName = cms.untracked(cms.string(""))

process.writeData2 = cms.OutputModule("PoolOutputModule")
process.writeData2.fileName = cms.untracked(cms.string("writeData2.root"))
process.writeData2.logicalFileName = cms.untracked(cms.string(""))

process.writeData3 = cms.OutputModule("PoolOutputModule")
process.writeData3.fileName = cms.untracked(cms.string("writeData3.root"))
process.writeData3.logicalFileName = cms.untracked(cms.string(""))

process.endPath = cms.EndPath(process.writeData1 * process.writeData2 * process.writeData3)


if __name__ == '__main__':
    #convienience util to push the file to ConfigCache
    import inspect
    import pickle
    import os
    
    def findMe():
        pass
    thisFile = inspect.getsourcefile(findMe)
    
    pickleConfig = "%s/pickledPSet.pkl" % os.getcwd()
    handle = open(pickleConfig, 'w')
    pickle.dump(process, handle)
    handle.close()

    
    
    from PSetTweaks.WMTweak import makeTweak
    from WMCore.Cache.ConfigCache import WMConfigCache
    
    localCache = WMConfigCache("config_cache", "127.0.0.1:5984")
    
    tweak = makeTweak(process)
    #outputModules = tweak.process.outputModules_
    
    docId,revId = localCache.addConfig( pickleConfig )
    localCache.addOriginalConfig( docId, revId, thisFile)
    localCache.addTweak(docId, revId, tweak.jsondictionary())
    
    
    os.remove(pickleConfig)    
    
    
    





