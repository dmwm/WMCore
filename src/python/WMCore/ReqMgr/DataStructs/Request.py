"""
Unlike ReqMgr1 defining Request and RequestSchema classes,
define just 1 class. Derived from Python dict and implementing
necessary conversion and validation extra methods possibly needed.


    since arguments validation #4705, arguments which are later
        validated during spec instantiation and which are not
        present in the request injection request, can't be defined
        here because their None value is not allowed in the spec.
        This is the case for e.g. DbsUrl, AcquisitionEra
        This module should probably define only absolutely
        necessary request parameters and not any optional ones.

"""
from __future__ import print_function, division
from builtins import range, object
from future.utils import viewitems, viewvalues, listvalues

import re
import time
from copy import deepcopy
from WMCore.REST.Auth import get_user_info
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE, ACTIVE_STATUS_FILTER


def initialize_request_args(request, config):
    """
    Request data class request is a dictionary representing
    a being injected / created request. This method initializes
    various request fields. This should be the ONLY method to
    manipulate request arguments upon injection so that various
    levels or arguments manipulation does not occur across several
    modules and across about 7 various methods like in ReqMgr1.

    request is changed here.
    """

    # user information for cert. (which is converted to cherry py log in)
    user = get_user_info()
    request["Requestor"] = user["login"]
    request["RequestorDN"] = user.get("dn", "unknown")
    # service certificates carry @hostname, remove it if it exists
    request["Requestor"] = request["Requestor"].split('@')[0]

    # assign first starting status, should be 'new'
    request["RequestStatus"] = REQUEST_START_STATE
    request["RequestTransition"] = [{"Status": request["RequestStatus"],
                                     "UpdateTime": int(time.time()), "DN": request["RequestorDN"]}]
    request["RequestDate"] = list(time.gmtime()[:6])

    # set the original priority when request is create
    request["PriorityTransition"] = [{"Priority": request["RequestPriority"],
                                      "UpdateTime": int(time.time()), "DN": request["RequestorDN"]}]
    # update the information from config
    request["CouchURL"] = config.couch_host
    request["CouchWorkloadDBName"] = config.couch_reqmgr_db
    request["CouchDBName"] = config.couch_config_cache_db

    generateRequestName(request)
    replaceDbsProdUrl(request)


def _replace_cloned_args(clone_args, user_args):
    """
    replace original arguments with user argument.
    If the value is dictionary format, overwrite only with specified arguments.
    If the original argument has simple value and user passes dictionary, completely replace to dictionary
    XXX this means LumiList won't remove other runs, only updates runs overwritten
    """
    for prop in user_args:
        if isinstance(user_args[prop], dict) and isinstance(clone_args.get(prop), dict):
            _replace_cloned_args(clone_args.get(prop, {}), user_args[prop])
        else:
            clone_args[prop] = user_args[prop]
    return


def replaceDbsProdUrl(requestArgs):
    """
    Function to update the DBS URL from the standard cmsweb.cern.ch
    to cmsweb-prod.cern.ch, running in the k8s infra
    :param requestArgs: dictionary with the request arguments
    :return: same dictionary object (with updated dbs url)
    """
    # TODO: this url conversion below can be removed in one year from now, thus March 2022
    if requestArgs.get("DbsUrl"):
        requestArgs["DbsUrl"] = requestArgs["DbsUrl"].replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
        requestArgs["DbsUrl"] = requestArgs["DbsUrl"].rstrip("/")


def initialize_clone(requestArgs, originalArgs, argsDefinition, chainDefinition=None):
    """
    Initialize arguments for a clone request by inheriting and overwriting argument
    from OriginalRequest.

    :param requestArgs: user-provided dictionary with override arguments
    :param originalArgs: original arguments retrieved for the workflow being cloned
    :param argsDefinition: arguments definition according to the workflow type being cloned
    :param chainDefinition: a dictionary containing the chain argument definition, for
    StepChain and TaskChain
    :return: dictionary with original args filtered out, as per the spec definition. And on
     top of that, user arguments added/replaced in the dictionary.
    """
    chainDefinition = chainDefinition or {}
    chainPattern = r'(Task|Step)\d{1,2}'

    if originalArgs == argsDefinition:
        # then it's a clone/ACDC of another ACDC, nothing else to do
        cloneArgs = originalArgs
    else:
        cloneArgs = {}
        for topKey, topValue in viewitems(originalArgs):
            # order of this if-else matters because Step1/Task1 is a known argument
            if re.match(chainPattern, topKey):
                cloneArgs.setdefault(topKey, {})
                # remove unsupported keys from inner Step/Task dict
                for innerKey in topValue:
                    if innerKey in chainDefinition:
                        cloneArgs[topKey][innerKey] = topValue[innerKey]
            # accepts floating Skim args for ReReco
            elif topKey in argsDefinition or topKey.startswith('Skim'):
                cloneArgs[topKey] = topValue

    # apply user override arguments at the end, such that it's validated at spec level
    incrementProcVer(cloneArgs, requestArgs)
    _replace_cloned_args(cloneArgs, requestArgs)
    replaceDbsProdUrl(cloneArgs)

    return cloneArgs


def incrementProcVer(cloneArgs, requestArgs):
    """
    Increment the ProcessingVersion value for any requests cloned via
    clone API, except if it's a Resubmission request.
    TODO: ProcVer can be a dict at top level, until this #6881 gets fixed
    """
    # if either the parent or the new workflow is a Resubmission, we shall not increment ProcVer
    if cloneArgs['RequestType'] == 'Resubmission' or requestArgs.get('RequestType') == 'Resubmission':
        return
    for key in cloneArgs:
        if key == 'ProcessingVersion':
            if isinstance(cloneArgs[key], int):
                cloneArgs[key] += 1
            elif isinstance(cloneArgs[key], dict):
                for taskname in cloneArgs[key]:
                    cloneArgs[key][taskname] += 1
        elif isinstance(cloneArgs[key], dict) and 'ProcessingVersion' in cloneArgs[key]:
            cloneArgs[key]['ProcessingVersion'] += 1
    return


def generateRequestName(request):
    currentTime = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
    seconds = int(10000 * (time.time() % 1.0))

    request["RequestName"] = "%s_%s" % (request["Requestor"], request.get("RequestString"))
    request["RequestName"] += "_%s_%s" % (currentTime, seconds)


def protectedLFNs(requestInfo):
    reqData = RequestInfo(requestInfo)
    result = []
    if reqData.andFilterCheck(ACTIVE_STATUS_FILTER):
        outs = requestInfo.get('OutputDatasets', [])
        base = requestInfo.get('UnmergedLFNBase', '/store/unmerged')
        for out in outs:
            dsn, ps, tier = out.split('/')[1:]
            acq, rest = ps.split('-', 1)
            dirPath = '/'.join([base, acq, dsn, tier, rest])
            result.append(dirPath)
    return result


class RequestInfo(object):
    """
    Wrapper class for Request data
    """

    def __init__(self, requestData):
        self.data = requestData

    def _maskTaskStepChain(self, prop, chain_name, default=None):

        propExist = False
        numLoop = self.data["%sChain" % chain_name]
        for i in range(numLoop):
            if prop in self.data["%s%s" % (chain_name, i + 1)]:
                propExist = True
                break

        defaultValue = self.data.get(prop, default)

        if propExist:
            result = set()
            for i in range(numLoop):
                chain_key = "%s%s" % (chain_name, i + 1)
                chain = self.data[chain_key]
                if prop in chain:
                    foundValue = chain[prop]
                else:
                    if isinstance(defaultValue, dict):
                        foundValue = defaultValue.get(chain_key, None)
                    else:
                        foundValue = deepcopy(defaultValue)

                if foundValue not in [None, ""]:
                    if isinstance(foundValue, (list, set)):
                        result.update(foundValue)
                    else:
                        result.add(foundValue)
            return list(result)
        else:
            # property which can't be task or stepchain property but in dictionary format
            exculdePropWithDictFormat = ["LumiList", "AgentJobInfo"]
            if prop not in exculdePropWithDictFormat and isinstance(defaultValue, dict):
                return listvalues(defaultValue)
            else:
                return defaultValue

    def get(self, prop, default=None):
        """
        gets the value when prop exist as one of the properties in the request document.
        In case TaskChain, StepChain workflow it searches the property in Task/Step level
        """

        if "TaskChain" in self.data:
            return self._maskTaskStepChain(prop, "Task", default)
        elif "StepChain" in self.data:
            return self._maskTaskStepChain(prop, "Step", default)
        elif prop in self.data:
            return self.data[prop]
        else:
            return default

    def andFilterCheck(self, filterDict):
        """
        checks whether filterDict condition met.
        filterDict is the dict of key and value(list) format)
        i.e.
        {"RequestStatus": ["running-closed", "completed"],}
        If this request's RequestStatus is either "running-closed", "completed",
        return True, otherwise False
        """
        for key, value in viewitems(filterDict):
            # special case checks where key is not exist in Request's Doc.
            # It is used whether AgentJobInfo is deleted or not for announced status
            if value == "CLEANED" and key == "AgentJobInfo":
                if self.isWorkflowCleaned():
                    continue
                else:
                    return False

            if isinstance(value, dict):
                # TODO: need to handle dictionary comparison
                # For now ignore
                continue
            elif value in ["false", "False", "FALSE"]:
                value = False
            elif value in ["true", "True", "TRUE"]:
                value = True

            # Now make value list if value is not in list form including bool value
            if not isinstance(value, list):
                value = [value]

            reqValue = self.get(key)
            if reqValue is not None:
                if isinstance(reqValue, list):
                    if not set(reqValue).intersection(set(value)):
                        return False
                elif reqValue not in value:
                    return False
            else:
                return False
        return True

    def isWorkflowCleaned(self):
        """
        check whether workflow data is cleaned up from agent only checks the couchdb
        Since dbsbuffer data is not clean up we can't just check 'AgentJobInfo' key existence
        This all is only meaningfull if request status is right before end status.
        ["aborted-completed", "rejected", "announced"]
        DO NOT check if workflow status isn't among those status
        """
        if 'AgentJobInfo' in self.data:
            for agentRequestInfo in viewvalues(self.data['AgentJobInfo']):
                if agentRequestInfo.get("status", {}):
                    return False
        # cannot determine whether AgentJobInfo is cleaned or not when 'AgentJobInfo' key doesn't exist
        # Maybe JobInformation is not included but since it requested by above status assumed it returns True
        return True
