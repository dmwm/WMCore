from __future__ import (print_function, division)
import cherrypy
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str
from WMCore.ReqMgr.Utils.Validation import get_request_template_from_type

import WMCore.ReqMgr.Service.RegExp as rx
from WMCore.REST.Format import JSONFormat

from WMCore.Wrappers import JsonWrapper
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

class RequestSpec(RESTEntity):
    
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        validate_str("name", param, safe, rx.RX_REQUEST_NAME, optional=False)


    @restcall(formats = [('application/json', JSONFormat())])
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


    @restcall(formats = [('application/json', JSONFormat())])
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
            raise cherrypy.HTTPError(404, "Cannot find workload: % "+ name)
        
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
        return

        
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        self._validate_args(param, safe)


    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, name):
        """
        getting job splitting algorithm.
        
        :arg str name: name to appear in the result message.
        :returns: row with response, here 1 item list with message.
        
        """
        
        helper = WMWorkloadHelper()
        try:
            helper.loadSpecFromCouch(self.reqdb_url, name)
        except Exception:
            raise cherrypy.HTTPError(404, "Cannot find workload: % "+ name)
        
        splittingDict = helper.listJobSplittingParametersByTask(performance = False)
        taskNames = sorted(splittingDict.keys())

        splitInfo = []
        for taskName in taskNames:
            splitInfo.append({"splitAlgo": splittingDict[taskName]["algorithm"],
                              "splitParams": splittingDict[taskName],
                              "taskType": splittingDict[taskName]["type"],
                              "taskName": taskName})
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
        splittingInfo = JsonWrapper.loads(data)
        
        for taskInfo in splittingInfo:
            splittingTask = taskInfo["taskName"]
            splittingAlgo = taskInfo["splitAlgo"]
            submittedParams = taskInfo["splitParams"]
            splitParams = {}
            if splittingAlgo == "FileBased":
                splitParams["files_per_job"] = int(submittedParams["files_per_job"])
            elif splittingAlgo == "TwoFileBased":
                splitParams["files_per_job"] = int(submittedParams["two_files_per_job"])
            elif splittingAlgo == "LumiBased":
                splitParams["lumis_per_job"] = int(submittedParams["lumis_per_job"])
                if "halt_job_on_file_boundaries" in submittedParams:
                    if str(submittedParams["halt_job_on_file_boundaries"]) == "True":
                        splitParams["halt_job_on_file_boundaries"] = True
                    else:
                        splitParams["halt_job_on_file_boundaries"] = False
            elif splittingAlgo == "EventAwareLumiBased":
                splitParams["events_per_job"] = int(submittedParams["events_per_job"])
                splitParams["max_events_per_lumi"] = int(submittedParams["max_events_per_lumi"])
                if "halt_job_on_file_boundaries" in submittedParams:
                    if str(submittedParams["halt_job_on_file_boundaries"]) == "True":
                        splitParams["halt_job_on_file_boundaries"] = True
                    else:
                        splitParams["halt_job_on_file_boundaries"] = False
            elif splittingAlgo == "EventBased":
                splitParams["events_per_job"] = int(submittedParams["events_per_job"])
                if "events_per_lumi" in submittedParams:
                    splitParams["events_per_lumi"] = int(submittedParams["events_per_lumi"])
                if "lheInputFiles" in submittedParams:
                    if str(submittedParams["lheInputFiles"]) == "True":
                        splitParams["lheInputFiles"] = True
                    else:
                        splitParams["lheInputFiles"] = False
            elif splittingAlgo == "Harvest":
                splitParams["periodic_harvest_interval"] = int(submittedParams["periodic_harvest_interval"])
            elif 'Merge' in splittingTask:
                for field in ['min_merge_size', 'max_merge_size', 'max_merge_events', 'max_wait_time']:
                    if field in submittedParams:
                        splitParams[field] = int(submittedParams[field])
            if "include_parents" in submittedParams.keys():
                if str(submittedParams["include_parents"]) == "True":
                    splitParams["include_parents"] = True
                else:
                    splitParams["include_parents"] = False
            
            helper = WMWorkloadHelper()
            try:
                helper.loadSpecFromCouch(self.reqdb_url, name)
            except Exception:
                raise cherrypy.HTTPError(404, "Cannot find workload: % "+ name)
            
            helper.setJobSplittingParameters(splittingTask, splittingAlgo, splitParams)
        
        url = "%s/%s" % (self.reqdb_url, name)
        result = helper.saveCouchUrl(url)    
        return result