from __future__ import (division, print_function)

import socket
import time
import copy
import WMCore
from WMCore.Wrappers import JsonWrapper
from WMCore.Services.Service import Service

def _changeRunStruct(dictData, sourceKey):
    if sourceKey in dictData:
        # convert output module
        for key, valueList in dictData[sourceKey].items():
            for valueDict in valueList:
                if "runs" in valueDict:
                    newValue = [{"runNumber": run, "lumis": lumis}  for run, lumis in valueDict["runs"].items() ]
                    valueDict["runs"] = newValue
    return

def _convertDictToList(dictData, sourceKey, keyName, valueName):
    _changeRunStruct(dictData, sourceKey)
    newData = [{keyName: key, valueName: value}  for key, value in dictData[sourceKey].items() ]
    dictData[sourceKey] = newData

def convertToArchiverFormat(fwjr):
    """
    """
    newFWJR = copy.deepcopy(fwjr)
    if newFWJR.get("steps", False):
        steps = newFWJR['steps']
        for key in steps:
            if "output" in steps[key]:
                _convertDictToList(steps[key], 'output', "outputModule", "value")
            if "input" in steps[key]:
                _changeRunStruct(steps[key], 'input')
    return newFWJR
    
class WMArchiver(Service):
    """
    This is skelton class which need be implemented.
    """
    def __init__(self, url, header = {}):
        """
        responseType will be either xml or json
        """

        httpDict = {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        httpDict.setdefault("accept_type", "application/json")
        httpDict.update(header)
        self.encoder = JsonWrapper.dumps
        Service.__init__(self, httpDict)        
    
    def createArchiverDoc(self, job_id, fwjr):
        """
        job_id is jobid + retry count same as couch db _id
        """
        newfwjr = convertToArchiverFormat(fwjr)
        # append meta data in fwjr
        newfwjr['meta_data'] = {'agent_ver': WMCore.__version__,
                             'host': socket.gethostname().lower(),
                             'fwjr_id': job_id,
                             'ts': int(time.time())
                             }
        return newfwjr
    
    def archiveData(self, data):
        return self["requests"].post('', {'data': data})[0]['result']