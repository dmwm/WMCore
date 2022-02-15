from __future__ import (division, print_function)
from builtins import str
from future.utils import viewitems, listvalues, listitems

import copy
import socket
from collections import defaultdict

# From top level

WMARCHIVE_TOP_LEVEL_REMOVE_OUTSIDE_LAYER = ["steps"]
# convert data format under stpes["cmsRun1"/"logArch1"/"stageOut1"]["output"]
WMARCHIVE_REMOVE_OUTSIDE_LAYER = ["checksum", "dataset"]
# convert to list from str
WMARCHIVE_CONVERT_TO_LIST = ["lfn"]

WMARCHIVE_DATA_MAP = {"inputPath": "inputDataset",
                      "lfn": "outputLFNs", "input": "inputLFNs"}

WMARCHIVE_REMOVE_FIELD = ["user_dn", "user_vogroup", "user_vorole"]
WMARCHIVE_COMBINE_FIELD = {"outputDataset": ["primaryDataset", "processedDataset", "dataTier"]}

WMARCHIVE_LFN_REF_KEY = ["lfn", "files"]
WMARCHIVE_PFN_REF_KEY = []
WMARCHIVE_FILE_REF_KEY = {"LFN": WMARCHIVE_LFN_REF_KEY}

ERROR_TYPE = {'exitCode': int}

PERFORMANCE_TYPE = {'cpu': {'AvgEventCPU': float,
                            'AvgEventTime': float,
                            'MaxEventCPU': float,
                            'MaxEventTime': float,
                            'MinEventCPU': float,
                            'MinEventTime': float,
                            'TotalEventCPU': float,
                            'TotalJobCPU': float,
                            'TotalJobTime': float,
                            'EventThroughput': float,
                            'TotalLoopCPU': float,
                            'TotalInitTime': float,
                            'TotalInitCPU': float,
                            'NumberOfThreads': int,
                            'NumberOfStreams': int},
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

TOP_LEVEL_STEP_DEFAULT = {'analysis': {},
                          'cleanup': {},
                          'logs': {},
                          'errors': [],
                          'input': [],
                          'output': [],
                          'performance': {}
                          }

# only composed value need to bs set default value
STEP_DEFAULT = {  # 'name': '',
    'analysis': {},
    'cleanup': {},
    'logs': {},
    'errors': [],
    'input': [{  # 'catalog': '',
        # 'events': -1,
        # 'guid': '',
        # 'input_source_class': '',
        # 'input_type': '',
        # 'lfn': -1,
        # 'module_label': '',
        # 'pfn': -1,
        'runs': []}],
    'output': [{  # 'acquisitionEra': '',
        # 'adler32': '',
        # 'applicationName': '',
        # 'applicationVersion': '',
        # 'async_dest': '',
        # 'branch_hash': '',
        # 'catalog': '',
        # 'cksum': '',
        # 'configURL': '',
        # 'events': -1,
        # 'globalTag': '',
        # 'guid': '',
        # 'inputDataset': '',
        'inputLFNs': [],
        # TODO change to empty string from None
        # 'location': '',
        # 'merged': False,
        # 'module_label': '',
        # 'output_module_class': '',
        # 'outputDataset': '',
        'outputLFNs': [],
        # 'prep_id': '',
        # 'processingStr': '',
        # 'processingVer': -1,
        'runs': [],
        # 'size': -1,
        # 'validStatus': '',
        # "SEName": '',
        # "PNN": '',
        # "GUID": '',
        # 'StageOutCommand': ''
    }],
    'performance': {'cpu': {},
                    'memory': {},
                    'multicore': {},
                    'storage': {}},
    # 'site': 'T2_CH_CERN',
    # 'start': 1454569735,
    # 'status': 0,
    # 'stop': 1454569736
}


def cleanStep(idict):
    """
    perform clean-up of PFNs attributes in place for given dictionary

    :param idict: a FWJR report dictionary
    :return: a dictionary without PFNs
    """
    for step in ['input', 'output']:
        data = idict.get(step, {})
        for key, values in data.items():
            for elem in values:
                for skip in ['pfn', 'InputPFN', 'OutputPFN', 'inputpfns']:
                    if skip in elem:
                        del elem[skip]
            data[key] = values
    return idict

def combineDataset(dataset):
    dataset["outputDataset"] = "/%s/%s/%s" % (dataset.pop("primaryDataset"),
                                              dataset.pop("processedDataset"),
                                              dataset.pop("dataTier"))
    return dataset


def changeRunStruct(runDict):
    runList = []
    for run in runDict:
        singleRun = {"runNumber": int(run)}
        singleRun.update({'lumis': [], 'eventsPerLumi': []})
        runList.append(singleRun)

    return runList


def _changeToFloat(value):
    if value in ["-nan", "nan", "inf", ""]:
        return -1.0
    return float(value)


def _validateTypeAndSetDefault(sourceDict, stepDefault):
    # check primitive time and remvoe if the values is composite type.
    for key, value in listitems(sourceDict):  # ACHTUNG! dict size changes while iterating
        if key not in stepDefault and value in [[], {}, None, "None"]:
            del sourceDict[key]

    # set missing composite type defaut.
    for category in stepDefault:
        if (category not in sourceDict) or (category in sourceDict and not sourceDict[category]):
            sourceDict[category] = stepDefault[category]


def changePerformanceStruct(perfDict):
    return [{"pName": prop, "value": _changeToFloat(value)} for prop, value in viewitems(perfDict)]


def changeToList(aDict):
    return [{"prop": prop, "value": value} for prop, value in viewitems(aDict)]


def convertInput(inputList):
    for inputDict in inputList:
        if "runs" in inputDict:
            inputDict['runs'] = changeRunStruct(inputDict["runs"])

        _validateTypeAndSetDefault(inputDict, STEP_DEFAULT['input'][0])

    return inputList


def typeCastError(errorList):
    for errorDict in errorList:
        for key in errorDict:
            if key in ERROR_TYPE:
                value = errorDict[key]
                errorDict[key] = ERROR_TYPE[key](value)
    return errorList


def typeCastPerformance(performDict):
    newPerfDict = defaultdict(dict)
    for key in PERFORMANCE_TYPE:
        if key in performDict:
            for param in PERFORMANCE_TYPE[key]:
                if param in performDict[key]:
                    try:
                        value = performDict[key][param]
                        if value in ["-nan", "nan", "inf", ""]:
                            value = -1
                        if PERFORMANCE_TYPE[key][param] == int:
                            # the received value comes from CMSSW FWJR and its type is string
                            # Although type is int, CMSSW FWJR string is constructed as float i.e. "3.0"
                            # In that case we convert to float first before type cast to int.
                            # since int("3.0") will raise an exception but int(float("3.0") won't
                            value = float(value)
                        newPerfDict[key][param] = PERFORMANCE_TYPE[key][param](value)
                    except ValueError as ex:
                        newPerfDict[key][param] = PERFORMANCE_TYPE[key][param](-1)
                        print("key: %s, param: %s, value: %s \n%s" % (key, param,
                                                                      performDict[key][param], str(ex)))
    return dict(newPerfDict)


def convertOutput(outputList):
    newOutputList = []
    for itemList in outputList:
        newOutputList.extend(itemList)

    for outDict in newOutputList:
        for field in WMARCHIVE_REMOVE_FIELD:
            if field in outDict:
                del outDict[field]

        for field in WMARCHIVE_CONVERT_TO_LIST:
            if field in outDict and isinstance(outDict[field], (str, bytes)):
                outDict[field] = [outDict[field]]

        for oldKey, newKey in viewitems(WMARCHIVE_DATA_MAP):
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

        if "location" in outDict and isinstance(outDict["location"], list):
            if outDict["location"]:
                outDict["location"] = outDict["location"][0]
            else:
                outDict["location"] = ""

        _validateTypeAndSetDefault(outDict, STEP_DEFAULT['output'][0])

    return newOutputList


def convertStepValue(stepValue):
    if "status" in stepValue:
        if stepValue["status"] == "Failed":
            stepValue["status"] = 1
        else:
            stepValue["status"] = int(stepValue["status"])

    if "errors" in stepValue:
        if not stepValue['errors']:
            stepValue['errors'] = []
        else:
            typeCastError(stepValue['errors'])

    input_keys = ['source', 'logArchives']
    if "input" in stepValue:
        if not stepValue['input']:
            # if empty convert to list from {}
            stepValue['input'] = []

        elif len(stepValue['input']) > 1:
            # assume only one input value
            raise Exception("more than one input value %s" % list(stepValue['input']))

        elif list(stepValue['input'].keys())[0] in input_keys:
            stepValue['input'] = convertInput(stepValue['input'][list(stepValue['input'])[0]])

        else:
            raise Exception("Unknow iput key %s" % list(stepValue['input']))

    if "output" in stepValue:
        # remove output module name layer
        stepValue['output'] = convertOutput(listvalues(stepValue['output']))

    if "performance" in stepValue:
        stepValue["performance"] = typeCastPerformance(stepValue["performance"])
        # If it needs to chnage to list format replace to this
        # for category in stepValue["performance"]:
        #    stepValue["performance"][category] = changePerformanceStruct(stepValue["performance"][category])

        _validateTypeAndSetDefault(stepValue["performance"], STEP_DEFAULT["performance"])

    # If structure need to be changed with this uncomments
    # listConvKeys = ['analysis', 'cleanup', 'logs', 'parameters']
    # for key in listConvKeys:
    #    stepValue[key] = changeToList(stepValue[key])

    return stepValue

def convertSteps(steps):
    stepList = []
    for key, value in viewitems(steps):
        stepItem = {}
        stepItem['name'] = key
        value = cleanStep(value)
        stepItem.update(convertStepValue(value))
        _validateTypeAndSetDefault(stepItem, TOP_LEVEL_STEP_DEFAULT)
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
        for key, value in viewitems(fwjr):
            addKeyFlag = False

            for fileType, keyList in viewitems(WMARCHIVE_FILE_REF_KEY):
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
        for key, value in viewitems(fwjr):
            for fileType in WMARCHIVE_FILE_REF_KEY:
                if key in fArrayRef[fileType]:
                    if isinstance(value, list):
                        for fileName in value:
                            fArray[fileType].add(fileName)
                    else:  # this should be string
                        fArray[fileType].add(value)
            createFileArray(value, fArray, fArrayRef)
    elif isinstance(fwjr, list):
        for item in fwjr:
            createFileArray(item, fArray, fArrayRef)
    else:
        return


def changeToFileRef(fwjr, fArray, fArrayRef):
    if isinstance(fwjr, dict):
        for key, value in listitems(fwjr):  # ACHTUNG! dict values change while iterating
            for fileType in WMARCHIVE_FILE_REF_KEY:
                if key in fArrayRef[fileType]:
                    if isinstance(value, list):
                        newRef = []
                        for fileName in value:
                            index = fArray[fileType].index(fileName)
                            newRef.append(index)
                    else:  # this should be string
                        newRef = fArray[fileType].index(value)
                    fwjr[key] = newRef
            changeToFileRef(value, fArray, fArrayRef)
    elif isinstance(fwjr, list):
        for item in fwjr:
            changeToFileRef(item, fArray, fArrayRef)
    else:
        return


def createArchiverDoc(job, version=None):
    """
    job_id is jobid + retry count same as couch db _id
    """

    job_id = job["id"]
    fwjr = job['doc']["fwjr"]
    jobtype = job['doc']["jobtype"]
    jobstate = job['doc']['jobstate']
    create_ts = job['doc']['timestamp']
    newfwjr = convertToArchiverFormat(fwjr)

    fArrayRef = {}
    fArray = {}
    for fileType in WMARCHIVE_FILE_REF_KEY:
        fArrayRef[fileType] = set()
        fArray[fileType] = set()

    createFileArrayRef(newfwjr, fArrayRef)

    for fileType in WMARCHIVE_FILE_REF_KEY:
        fArrayRef[fileType] = list(fArrayRef[fileType])

    createFileArray(newfwjr, fArray, fArrayRef)

    for fileType in WMARCHIVE_FILE_REF_KEY:
        fArray[fileType] = list(fArray[fileType])

    changeToFileRef(newfwjr, fArray, fArrayRef)

    # convert to fwjr format

    for fileType in WMARCHIVE_FILE_REF_KEY:
        newfwjr["%sArrayRef" % fileType] = fArrayRef[fileType]
        newfwjr["%sArray" % fileType] = fArray[fileType]

    if version is None:
        # add this trry to remove the dependency on WMCore code.
        import WMCore
        version = WMCore.__version__
    # append meta data in fwjr
    wnName = ""
    if "WorkerNodeInfo" in fwjr:
        wnName = fwjr["WorkerNodeInfo"].get("HostName", "")

    newfwjr['meta_data'] = {'agent_ver': version,
                            'host': socket.gethostname().lower(),
                            'wn_name': wnName,
                            'fwjr_id': job_id,
                            'jobtype': jobtype,
                            'jobstate': jobstate,
                            'ts': create_ts
                            }
    return newfwjr
