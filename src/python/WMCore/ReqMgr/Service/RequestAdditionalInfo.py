from __future__ import (print_function, division)

import json

import cherrypy

import WMCore.ReqMgr.Service.RegExp as rx
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str
from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache
from WMCore.ReqMgr.Utils.Validation import get_request_template_from_type
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def format_algo_web_list(task_name, task_type, split_param):
    
    algo_config = ReqMgrConfigDataCache.getConfig("EDITABLE_SPLITTING_PARAM_CONFIG")
    
    fdict = {"taskName": task_name}
    fdict["taskType"] = task_type
    default_algo = split_param["algorithm"]
    algo_list = algo_config["algo_list_by_types"][task_type]
    param_list = []
    
    if default_algo in algo_list:
        new_param = {"algorithm": default_algo}
        for key, value in split_param.items():
            if key in algo_config["algo_params"][default_algo]:
                new_param[key] = value
        param_list.append(new_param) 
    elif default_algo == "":
        raise cherrypy.HTTPError(400, "Algorithm name is empty: %s" % split_param)
    else:
        param_list.append(split_param)
    
    # If task type is merge don't allow change the algorithm
    if fdict["taskType"] != "Merge":
        for algo in algo_list:
            if algo != default_algo:
                param = {"algorithm": algo}
                param.update(algo_config["algo_params"][algo])
                param_list.append(param)
                
    fdict["splitParamList"] = param_list
    return fdict

def create_web_splitting_format(split_info):
    web_form = []
    for sp in split_info:
        # skip Cleanup and LogCollect: don't allow change the param
        if sp["taskType"] not in ["Cleanup", "LogCollect"]:
            web_form.append(format_algo_web_list(sp["taskName"], sp["taskType"], 
                                               sp["splitParams"]))
    return web_form

def _validate_split_param(split_algo, split_param):
    """
    validate param for editing, also returns param type
    """
    algo_config = ReqMgrConfigDataCache.getConfig("EDITABLE_SPLITTING_PARAM_CONFIG")
    
    valid_params = algo_config["algo_params"][split_algo]
    if split_param in valid_params:
        if isinstance(valid_params[split_param], bool):
            cast_type = bool
        else:
            cast_type = int
        return (True, cast_type)
    else:
        return (False, None)
            
def _assign_if_key_exsit(key, original_params, return_params, cast_type):
    if key in original_params:
        if cast_type == None:
            return_params[key] = original_params[key]
        elif cast_type == bool:
            if str(original_params[key]).lower() == "true":
                return_params[key] = True
            else:
                return_params[key] = False
        else:
            return_params[key] = cast_type(original_params[key])

class RequestSpec(RESTEntity):
    
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        validate_str("name", param, safe, rx.RX_REQUEST_NAME, optional=False)


    @restcall(formats = [('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, name):
        """
        Spec templete API call.
        
        :arg str name: name to appear in the result message.
        :returns: row with response, here 1 item list with message.
        
        """
        result = get_request_template_from_type(name)
        return [result]
    
class WorkloadConfig(RESTEntity):
    
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqdb_url = "%s/%s" % (config.couch_host, config.couch_reqmgr_db)
    
    def _validate_args(self, param, safe):
        # TODO: need proper validation but for now pass everything
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["name"] = param.args[0]
            param.args.pop()
        return

        
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        
        self._validate_args(param, safe)


    @restcall(formats = [('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, name):
        """
        Workload config world API call.
        
        :arg str name: name to appear in the result message.
        :returns: row with response.
        
        """
        
        helper = WMWorkloadHelper()
        try:
            helper.loadSpecFromCouch(self.reqdb_url, name)
        except Exception:
            raise cherrypy.HTTPError(404, "Cannot find workload: %s" % name)
        
        return str(helper.data)

class WorkloadSplitting(RESTEntity):
    
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqdb_url = "%s/%s" % (config.couch_host, config.couch_reqmgr_db)
    
    
    def _validate_args(self, param, safe):
        # TODO: need proper validation but for now pass everything
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["name"] = param.args[0]
            param.args.pop()
        elif args_length == 2 and  param.args[0] == "web_form":
            safe.kwargs["web_form"] = True
            safe.kwargs["name"] = param.args[1]
            param.args.pop()
            param.args.pop()
        return

        
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        self._validate_args(param, safe)


    @restcall(formats = [('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, name, web_form=False):
        """
        getting job splitting algorithm.
        
        :arg str name: name to appear in the result message.
        :returns: row with response, here 1 item list with message.
        
        """
        
        helper = WMWorkloadHelper()
        try:
            helper.loadSpecFromCouch(self.reqdb_url, name)
        except Exception:
            raise cherrypy.HTTPError(404, "Cannot find workload: %s" % name)
        
        splittingDict = helper.listJobSplittingParametersByTask(performance = False)
        taskNames = sorted(splittingDict.keys())

        splitInfo = []
        for taskName in taskNames:
            splitInfo.append({"splitAlgo": splittingDict[taskName]["algorithm"],
                              "splitParams": splittingDict[taskName],
                              "taskType": splittingDict[taskName]["type"],
                              "taskName": taskName})
        if web_form:
            splitInfo = create_web_splitting_format(splitInfo)
            
        return splitInfo
    
    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self, name):
        """
        Parse job splitting parameters sent from the splitting parameter update
        page.  Pull down the request and modify the new spec applying the
        updated splitting parameters.
        """
        
        data = cherrypy.request.body.read()
        splittingInfo = json.loads(data)
        
        for taskInfo in splittingInfo:
            splittingTask = taskInfo["taskName"]
            splittingAlgo = taskInfo["splitAlgo"]
            submittedParams = taskInfo["splitParams"]
            splitParams = {}
            for param in submittedParams:
                validFlag, castType = _validate_split_param(splittingAlgo, param)
                if validFlag:
                    _assign_if_key_exsit(param, submittedParams, splitParams, castType)
                else:
                    #TO Maybe raise the error messge
                    pass
            
            #TODO: this is only gets updated through script. Maybe we should disallow it.
            _assign_if_key_exsit("include_parents", submittedParams, splitParams, bool)
            
            helper = WMWorkloadHelper()
            try:
                helper.loadSpecFromCouch(self.reqdb_url, name)
            except Exception:
                raise cherrypy.HTTPError(404, "Cannot find workload: %s" % name)
            
            helper.setJobSplittingParameters(splittingTask, splittingAlgo, splitParams)
            
            # Not sure why it needs to updated per each task but if following lines are outside the loop
            # it doesn't work
            url = "%s/%s" % (self.reqdb_url, name)
            result = helper.saveCouchUrl(url)    
        return result
