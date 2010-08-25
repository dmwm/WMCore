#!/usr/bin/env python
# encoding: utf-8
"""
Serialiser.py

Created by Dave Evans on 2010-02-23.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os


from WMCore.Algorithms.ParseXMLFile import coroutine

def serialiseCollectionDriver(service, inputColl, target):
    """
    _serialiseCollectionDriver_
    
    Top level serialiser driver that feeds the coroutines
    """
    result = {
        "filesets" : {},
        "owner" : inputColl.owner,
        "group" : inputColl.group,
        "name"  : inputColl.name ,        
        }   
    
    #TODO
    #collectionDoc = service.createCollection(result)
    #
    for fileset in inputColl.filesets.values():
        target.send( (service, fileset, result) )
    return result

def serialiseFilesetDriver(service, inputFileset, target):
    """
    _serialiseFilesetDriver_
    
    top level driver that serialises a fileset
    WhatIf: yield files?
    
    """
    result = {"fileset": {
        "files" : [],
        "dataset" : inputFileset.dataset,
        "collection" : inputFileset.collection,
        }
    }
    for fileElem in inputFileset.files:
        targets['File'].send( (fileElem, result['fileset']['files']))
    
    

        
@coroutine    
def processFileset(targets):
    """
    _processFileset_
    
    Fileset handling coroutine
    
    """
    while True:
        service, inputSet, collectionDict = (yield)
        result = collectionDict['filesets']
        result[inputSet.name] = {
            "dataset" : inputSet.dataset,
            "collection" : inputSet.collection,
            "files" : [],
        }
        #TODO:
        #document = service.createFilesetDoc(result)
        filelist = result[inputSet.name]['files']
        for fileElem in inputSet.files:
            targets['File'].send( (fileElem, filelist))
        #service.commit(document)
        
@coroutine
def processFile(targets):
    """
    _processFile_
    
    File handling coroutine

    """
    while True:
        inputFile, result = (yield)
        fileDict = {'runs': {}}
        result.append(fileDict)
        for key, value in inputFile.items():
            if key in ("lfn", "events", "size"):
                fileDict[key] = value
            elif key in targets.keys(): 
                targets[key].send( (value, fileDict))  
            else:
                continue
            
@coroutine
def processRun(targets):
    """
    _processRun_
    
    Run handling coroutine
    """
    while True:
        inputRuns, fileDict = (yield)
        runs = fileDict['runs']
        for inputRun in inputRuns:
            runs[inputRun.run]  = inputRun.lumis
            


def serialiseCollection(collection):
    """
    serialiseCollection
    """
    
    fileHandlers = {"runs" : processRun({})}
    return serialiseCollectionDriver(None, collection, processFileset({"File" : processFile(fileHandlers) }))

def serialiseFileset(fileset):
    """
    _serialiseFileset_
    """
    fileHandlers = {"runs" : processRun({})}
    return serialiseCollectionDriver(None, collection, processFile(fileHandlers))
    
    

def main():
    from WMCore.ACDC.MakeFilesets import testCachedCollection    
  
    class DummySvc:
        pass

    #coll = testCachedCollection()
    
    #fileHandlers = {"runs" : processRun({})}
    
    #print serialiseCollectionDriver(DummySvc(), coll, processFileset({"File" : processFile(fileHandlers) }))
    


if __name__ == '__main__':
    

    
    main()

