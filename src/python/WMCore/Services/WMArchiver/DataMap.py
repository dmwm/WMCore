from __future__ import (division, print_function)

import socket
import time
import copy

# From top level

WMARCHIVE_TOP_LEVEL_REMOVE_OUTSIDE_LAYER = ["steps"]
# convert data format under stpes["cmsRun1"/"logArch1"/"stageOut1"]["output"]
WMARCHIVE_REMOVE_OUTSIDE_LAYER = ["checksum", "dataset"]
# convert to list from str
WMARCHIVE_CONVERT_TO_LIST = ["OutputPFN", "lfn"]

WMARCHIVE_DATA_MAP = {"OutputPFN": "outputPFNs", "inputPath": "inputDataset",
                      "lfn": "outputLFNs", "input": "inputLFNs", "inputpfns": "inputPFNs"}


WMARCHIVE_REMOVE_FIELD = ["InputPFN", "pfn", "user_dn", "user_vogroup", "user_vorole"]
WMARCHIVE_COMBINE_FIELD = {"outputDataset": ["primaryDataset", "processedDataset", "dataTier"]}

WMARCHIVE_LFN_REF_KEY = ["lfn", "files"]
WMARCHIVE_PFN_REF_KEY = ["pfn"]
WMARCHIVE_FILE_REF_KEY = {"LFN": WMARCHIVE_LFN_REF_KEY, 
                          "PFN": WMARCHIVE_PFN_REF_KEY}

PERFORMANCE_TYPE = {'cpu': {'AvgEventCPU': float,
                            'AvgEventTime': float,
                            'MaxEventCPU': float,
                            'MaxEventTime': float,
                            'MinEventCPU':  float,
                            'MinEventTime': float,
                            'TotalEventCPU': float,
                            'TotalJobCPU': float,
                            'TotalJobTime': float,
                            'EventThroughput': float,
                            'TotalLoopCPU': float},
                    'memory': {'PeakValueRss': float,
                               'PeakValueVsize': float},
                    'storage': {'readAveragekB': float,
                                'readCachePercentageOps': float,
                                'readMBSec': float,
                                'readMaxMSec': float,
                                'readNumOps': float,
                                'readPercentageOps': float,
                                'readTotalMB': float,    
                                'readTotalSecs': float,
                                'writeTotalMB': float,
                                'writeTotalSecs': float}}
                                       
def combineDataset(dataset):
    dataset["outputDataset"] = "/%s/%s/%s" % (dataset["primaryDataset"], dataset["processedDataset"], dataset["dataTier"])
    del dataset["primaryDataset"]
    del dataset["processedDataset"]
    del dataset["dataTier"]
    return dataset
    
def changeRunStruct(runDict):
    return [{"runNumber": int(run), "lumis": lumis}  for run, lumis in runDict.items() ]
    

def convertInput(inputList):
    for inputDict in inputList:
        if "runs" in inputDict:
            inputDict['runs'] = changeRunStruct(inputDict["runs"])
    return inputList

def typeCastPerformance(performDict):
    for key in performDict:
        for param in performDict[key]:
            if key in PERFORMANCE_TYPE:
                if param in PERFORMANCE_TYPE[key]:
                    if performDict[key][param] in ["-nan", "nan", "inf"]:
                        performDict[key][param] = PERFORMANCE_TYPE[key][param](-1)
                    else:
                        performDict[key][param] = PERFORMANCE_TYPE[key][param](performDict[key][param])
    return performDict
            
    
def convertOutput(outputList):
    newOutputList = []
    for itemList in outputList:
        newOutputList.extend(itemList)
    
    for outDict in newOutputList:
        for field in WMARCHIVE_REMOVE_FIELD:
            if field in outDict:
                del outDict[field]
                
        for field in WMARCHIVE_CONVERT_TO_LIST:
            if field in outDict and isinstance(outDict[field], basestring):
                outDict[field] = [outDict[field]]
            
        for oldKey, newKey in WMARCHIVE_DATA_MAP.items():
            if oldKey in outDict:
                outDict[newKey] = outDict[oldKey]
                del outDict[oldKey]
                
        if "runs" in outDict:
            outDict['runs'] = changeRunStruct(outDict["runs"])
        
        if "checksums" in outDict:
            outDict.update(outDict["checksums"])
            del outDict["checksums"]
        
        if "dataset" in outDict:
            outDict.update(combineDataset(outDict["dataset"]))
            del outDict["dataset"]
            
        for key, value in outDict.items():
            # set the default value for None to empty string
            if value == None or value == "None":
                outDict[key] = ""
            
            
    return newOutputList

def convertStepValue(stepValue):
    if "input" in stepValue:
        # remove source layer
        stepValue['input'] = convertInput(stepValue['input'].get('source', {}))
    if "output" in stepValue:
        # remove output module name layer
        stepValue['output'] = convertOutput(stepValue['output'].values())
    
    if "performance" in stepValue:
        typeCastPerformance(stepValue["performance"])
        
    return stepValue
            
def convertSteps(steps):
    stepList = []
    for key, value in steps.items(): 
        stepItem = {}
        stepItem['name'] = key
        stepItem.update(convertStepValue(value))
        stepList.append(stepItem)
    return stepList

def convertToArchiverFormat(fwjr):
    """
    """
    newFWJR = copy.deepcopy(fwjr)
    if "steps" in newFWJR:
        newFWJR["steps"] = convertSteps(newFWJR["steps"])

    return newFWJR

def createFileArrayRef(fwjr, fArrayRef):
    
    if isinstance(fwjr, list):
        for item in fwjr:
            createFileArrayRef(item, fArrayRef)
    elif isinstance(fwjr, dict):               
        for key, value in fwjr.items():
            addKeyFlag = False
            
            for fileType, keyList in WMARCHIVE_FILE_REF_KEY.items():
                for kw in keyList:
                    if kw in key.lower():
                        fArrayRef[fileType].add(key)
                        addKeyFlag = True
                    
            if not addKeyFlag:
                createFileArrayRef(value, fArrayRef)
    else:
        return
        

def createFileArray(fwjr, fArray, fArrayRef):
    
    if isinstance(fwjr, dict):               
        for key, value in fwjr.items():
            for fileType in WMARCHIVE_FILE_REF_KEY.keys():
                if key in fArrayRef[fileType]:
                    if isinstance(value, list):
                        for fileName in value:
                            fArray[fileType].add(fileName)
                    else: # this should be string
                        fArray[fileType].add(value)                                
            else:
                createFileArray(value, fArray, fArrayRef)                  
    elif isinstance(fwjr, list):
        for item in fwjr:
            createFileArray(item, fArray, fArrayRef)
    else:
        return

def changeToFileRef(fwjr, fArray, fArrayRef):
    
    if isinstance(fwjr, dict):               
        for key, value in fwjr.items():
            for fileType in WMARCHIVE_FILE_REF_KEY.keys():
                if key in fArrayRef[fileType]:
                    if isinstance(value, list):
                        newRef = []
                        for fileName in value:
                            index = fArray[fileType].index(fileName)
                            newRef.append(index)
                    else: # this should be string
                        newRef = fArray[fileType].index(value)
                    fwjr[key] = newRef
            else:
                changeToFileRef(value, fArray, fArrayRef)
    elif isinstance(fwjr, list):
        for item in fwjr:
            changeToFileRef(item, fArray, fArrayRef)
    else:
        return
    
def createArchiverDoc(job_id, fwjr, version=None):
    """
    job_id is jobid + retry count same as couch db _id
    """
    newfwjr = convertToArchiverFormat(fwjr)
    
    fArrayRef = {}
    fArray = {}
    for fileType in WMARCHIVE_FILE_REF_KEY.keys():
        fArrayRef[fileType] = set()
        fArray[fileType] = set()

    createFileArrayRef(newfwjr, fArrayRef)
    
    for fileType in WMARCHIVE_FILE_REF_KEY.keys():
        fArrayRef[fileType] = list(fArrayRef[fileType])

    createFileArray(newfwjr, fArray, fArrayRef)
    
    for fileType in WMARCHIVE_FILE_REF_KEY.keys():
        fArray[fileType] = list(fArray[fileType])
        
    
    changeToFileRef(newfwjr, fArray, fArrayRef)
    
    #convert to fwjr format
    
    for fileType in WMARCHIVE_FILE_REF_KEY.keys():
        newfwjr["%sArrayRef" % fileType] = fArrayRef[fileType]
        newfwjr["%sArray" % fileType] = fArray[fileType]
    
    if version == None:
        # add this trry to remove the dependency on WMCore code.
        import WMCore
        version = WMCore.__version__
    # append meta data in fwjr
    newfwjr['meta_data'] = {'agent_ver': version,
                         'host': socket.gethostname().lower(),
                         'fwjr_id': job_id,
                         'ts': int(time.time())
                         }
    return newfwjr