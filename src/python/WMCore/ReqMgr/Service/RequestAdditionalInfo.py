from __future__ import (print_function, division)
from future.utils import viewitems

import json
import traceback

import cherrypy

import WMCore.ReqMgr.Service.RegExp as rx
from Utils.Utilities import strToBool
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str
from WMCore.REST.Error import MethodWithoutQueryString
from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache
from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue
from WMCore.ReqMgr.Utils.Validation import get_request_template_from_type
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def format_algo_web_list(task_name, task_type, split_param, algo_config):
    fdict = {"taskName": task_name}
    fdict["taskType"] = task_type
    default_algo = split_param["algorithm"]
    algo_list = algo_config["algo_list_by_types"][task_type]
    param_list = []

    if default_algo in algo_list:
        new_param = {"algorithm": default_algo}
        for key, value in viewitems(split_param):
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
    splitSettings = ReqMgrConfigDataCache.getConfig("EDITABLE_SPLITTING_PARAM_CONFIG")

    for sp in split_info:
        # skip Cleanup and LogCollect: don't allow change the param
        if sp["taskType"] not in ["Cleanup", "LogCollect"]:
            web_form.append(format_algo_web_list(sp["taskName"], sp["taskType"],
                                                 sp["splitParams"], splitSettings))
    return web_form


def create_updatable_splitting_format(split_info):
    """
    _create_updatable_splitting_format_

    Returns the workflow job splitting without parameters that
    cannot be updated in the POST call.
    """
    splitInfo = []
    splitSettings = ReqMgrConfigDataCache.getConfig("EDITABLE_SPLITTING_PARAM_CONFIG")

    for taskInfo in split_info:
        if taskInfo["taskType"] not in ["Cleanup", "LogCollect"]:
            splittingAlgo = taskInfo["splitAlgo"]
            submittedParams = taskInfo["splitParams"]
            splitParams = {}
            for param in submittedParams:
                validFlag, _ = _validate_split_param(splittingAlgo, param, splitSettings)
                if validFlag:
                    splitParams[param] = taskInfo["splitParams"][param]
            taskInfo["splitParams"] = splitParams
            splitInfo.append(taskInfo)
    return splitInfo


def _validate_split_param(split_algo, split_param, algo_config):
    """
    validate param for editing, also returns param type
    """

    valid_params = algo_config["algo_params"][split_algo]
    if split_param in valid_params:
        if isinstance(valid_params[split_param], bool):
            cast_type = bool
        else:
            cast_type = int
        return (True, cast_type)
    else:
        return (False, None)


def _assign_key_value(keyname, keyvalue, return_params, cast_type):
    if cast_type is None:
        return_params[keyname] = keyvalue
    elif cast_type == bool:
        try:
            return_params[keyname] = strToBool(keyvalue)
        except ValueError:
            msg = "%s expects a boolean value, you provided %s" % (keyname, keyvalue)
            raise cherrypy.HTTPError(400, msg)
    else:
        return_params[keyname] = cast_type(keyvalue)


class RequestSpec(RESTEntity):
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.

        """
        validate_str("name", param, safe, rx.RX_REQUEST_NAME, optional=False)

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
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
        else:
            raise MethodWithoutQueryString
        return

    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.

        """

        self._validate_args(param, safe)

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
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

    def _validate_get_args(self, param, safe):
        # TODO: need proper validation but for now pass everything
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["name"] = param.args[0]
            param.args.pop()
        elif args_length == 2 and param.args[0] == "web_form":
            safe.kwargs["web_form"] = True
            safe.kwargs["name"] = param.args[1]
            param.args.pop()
            param.args.pop()
        elif args_length == 2 and param.args[0] == "update_only":
            safe.kwargs["update_only"] = True
            safe.kwargs["name"] = param.args[1]
            param.args.pop()
            param.args.pop()
        else:
            raise MethodWithoutQueryString
        return

    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.

        """
        try:
            if method == 'GET':
                self._validate_get_args(param, safe)

            if method == 'POST':
                args_length = len(param.args)
                if args_length == 1:
                    safe.kwargs["name"] = param.args[0]
                    param.args.pop()
        except InvalidSpecParameterValue as ex:
            raise ex
        except Exception as ex:
            msg = traceback.format_exc()
            cherrypy.log("Error: %s" % msg)
            if hasattr(ex, "message"):
                if hasattr(ex.message, '__call__'):
                    msg = ex.message()
                else:
                    msg = str(ex)
            else:
                msg = str(ex)
            raise InvalidSpecParameterValue(msg)

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, name, web_form=False, update_only=False):
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

        splittingDict = helper.listJobSplittingParametersByTask(performance=False)
        taskNames = sorted(splittingDict.keys())
        splitInfo = []
        for taskName in taskNames:
            splitInfo.append({"splitAlgo": splittingDict[taskName]["algorithm"],
                              "splitParams": splittingDict[taskName],
                              "taskType": splittingDict[taskName]["type"],
                              "taskName": taskName})
        if web_form:
            splitInfo = create_web_splitting_format(splitInfo)
        elif update_only:
            splitInfo = create_updatable_splitting_format(splitInfo)

        return splitInfo

    @restcall(formats=[('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def post(self, name):
        """
        Parse job splitting parameters sent from the splitting parameter update
        page.  Pull down the request and modify the new spec applying the
        updated splitting parameters.
        """

        data = cherrypy.request.body.read()
        splittingInfo = json.loads(data)
        cherrypy.log("Updating job splitting for '%s' with these args: %s" % (name, splittingInfo))

        helper = WMWorkloadHelper()
        try:
            helper.loadSpecFromCouch(self.reqdb_url, name)
        except Exception:
            raise cherrypy.HTTPError(404, "Cannot find workload for: %s" % name)

        splitSettings = ReqMgrConfigDataCache.getConfig("EDITABLE_SPLITTING_PARAM_CONFIG")
        for taskInfo in splittingInfo:
            splittingTask = taskInfo["taskName"]
            splittingAlgo = taskInfo["splitAlgo"]
            submittedParams = taskInfo["splitParams"]
            splitParams = {}
            for param in submittedParams:
                validFlag, castType = _validate_split_param(splittingAlgo, param, splitSettings)
                if validFlag:
                    _assign_key_value(param, submittedParams[param], splitParams, castType)
                else:
                    msg = "Parameter '%s' is not supported in the algorithm '%s'" % (param, splittingAlgo)
                    raise cherrypy.HTTPError(400, msg)

            helper.setJobSplittingParameters(splittingTask, splittingAlgo, splitParams, updateOnly=True)

        # Now persist all these changes in the workload
        url = "%s/%s" % (self.reqdb_url, name)
        result = helper.saveCouchUrl(url)

        return result
