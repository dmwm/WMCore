"""
ReqMgr request handling.

"""

from builtins import str as newstr, bytes
from future.utils import viewitems, listvalues

import json
import traceback

import cherrypy

import WMCore.ReqMgr.Service.RegExp as rx
from WMCore.Database.CMSCouch import CouchError
from WMCore.Lexicon import sanitizeURL
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Validation import validate_str
from WMCore.REST.Auth import get_user_info
from WMCore.ReqMgr.DataStructs.AuthzByStatus import AuthzByStatus

from WMCore.ReqMgr.DataStructs.Request import RequestInfo
from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache
from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue
from WMCore.ReqMgr.DataStructs.RequestStatus import (REQUEST_STATE_LIST, REQUEST_STATE_TRANSITION,
                                                     ACTIVE_STATUS, check_allowed_transition)
from WMCore.ReqMgr.DataStructs.RequestType import REQUEST_TYPES
from WMCore.ReqMgr.Utils.Validation import (validate_request_create_args, validate_request_update_args,
                                            validate_clone_create_args, validateOutputDatasets,
                                            validate_request_priority, workqueue_stat_validation,
                                            isUserAllowed)
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue


class Request(RESTEntity):
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgrAuthzByStatus = AuthzByStatus(config.authz_by_status, config.authorized_roles)
        self.reqmgr_db = api.db_handler.get_db(config.couch_reqmgr_db)
        self.reqmgr_db_service = RequestDBWriter(self.reqmgr_db, couchapp="ReqMgr")
        # this need for the post validtiaon
        self.gq_service = WorkQueue(config.couch_host, config.couch_workqueue_db)

    def _validateGET(self, param, safe):
        # TODO: need proper validation but for now pass everything
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["name"] = param.args[0]
            param.args.pop()
            return

        no_multi_key = ["detail", "_nostale", "date_range", "common_dict"]
        for key, value in viewitems(param.kwargs):
            # convert string to list
            if key not in no_multi_key and isinstance(value, (newstr, bytes)):
                param.kwargs[key] = [value]

        detail = param.kwargs.get('detail', True)
        if detail in (False, "false", "False", "FALSE"):
            detail = False

        if "status" in param.kwargs and detail:
            for status in param.kwargs["status"]:
                if status.endswith("-archived"):
                    raise InvalidSpecParameterValue(
                        """Can't retrieve bulk archived status requests with detail option True,
                           set detail=false or use other search arguments""")

        for prop in list(param.kwargs):
            safe.kwargs[prop] = param.kwargs.pop(prop)
        return

    def _validateRequestBase(self, param, safe, valFunc, requestName=None):
        data = cherrypy.request.body.read()
        if data:
            request_args = json.loads(data)
        else:
            request_args = {}

        # first, verify if user is allowed to perform such action
        isUserAllowed(self.reqmgrAuthzByStatus, request_args)

        cherrypy.log('Updating request "%s" with these user-provided args: %s' % (requestName, request_args))

        # In case key args are also passed and request body also exists.
        # If the request.body is dictionary update the key args value as well
        if isinstance(request_args, dict):
            for prop in list(param.kwargs):
                request_args[prop] = param.kwargs.pop(prop)

            if requestName:
                request_args["RequestName"] = requestName
            request_args = [request_args]

        safe.kwargs['workload_pair_list'] = []
        for args in request_args:
            workload, r_args = valFunc(args, self.config, self.reqmgr_db_service, param)
            safe.kwargs['workload_pair_list'].append((workload, r_args))

    def _get_request_names(self, ids):
        "Extract request names from given documents"
        # cherrypy.log("request names %s" % ids)
        doc = {}
        if isinstance(ids, list):
            for rid in ids:
                doc[rid] = 'on'
        elif isinstance(ids, (newstr, bytes)):
            doc[ids] = 'on'

        docs = []
        for key in list(doc):
            if key.startswith('request'):
                rid = key.split('request-')[-1]
                if rid != 'all':
                    docs.append(rid)
                del doc[key]
        return docs

    def _getMultiRequestArgs(self, multiRequestForm):
        request_args = {}
        for prop in multiRequestForm:
            if prop == "ids":
                request_names = self._get_request_names(multiRequestForm["ids"])
            elif prop == "new_status":
                request_args["RequestStatus"] = multiRequestForm[prop]
            # remove this
            # elif prop in ["CustodialSites", "AutoApproveSubscriptionSites"]:
            #    request_args[prop] = [multiRequestForm[prop]]
            else:
                request_args[prop] = multiRequestForm[prop]
        return request_names, request_args

    def _validateMultiRequests(self, param, safe, valFunc):

        data = cherrypy.request.body.read()
        if data:
            request_names, request_args = self._getMultiRequestArgs(json.loads(data))
        else:
            # actually this is error case
            # cherrypy.log(str(param.kwargs))
            request_names, request_args = self._getMultiRequestArgs(param.kwargs)

            for prop in request_args:
                if prop == "RequestStatus":
                    del param.kwargs["new_status"]
                else:
                    del param.kwargs[prop]

            del param.kwargs["ids"]

        isUserAllowed(self.reqmgrAuthzByStatus, request_args)

        safe.kwargs['workload_pair_list'] = []

        for request_name in request_names:
            request_args["RequestName"] = request_name
            workload, r_args = valFunc(request_args, self.config, self.reqmgr_db_service, param)
            safe.kwargs['workload_pair_list'].append((workload, r_args))

        safe.kwargs["multi_update_flag"] = True

    def _getRequestNamesFromBody(self, safe):

        request_names = json.loads(cherrypy.request.body.read())
        safe.kwargs['workload_pair_list'] = request_names
        safe.kwargs["multi_names_flag"] = True

    def validate(self, apiobj, method, api, param, safe):
        # to make validate successful
        # move the validated argument to safe
        # make param empty
        # other wise raise the error
        try:
            if method == 'GET':
                self._validateGET(param, safe)

            elif method == 'PUT':
                args_length = len(param.args)

                if args_length == 1:
                    requestName = param.args[0]
                    param.args.pop()
                else:
                    requestName = None
                self._validateRequestBase(param, safe, validate_request_update_args, requestName)

            elif method == 'POST':
                args_length = len(param.args)
                if args_length == 2 and param.args[0] == "clone":
                    # handles clone workflow.- don't validtate args here
                    param.kwargs['OriginalRequestName'] = param.args[1]
                    param.args.pop()
                    param.args.pop()
                    self._validateRequestBase(param, safe, validate_clone_create_args)
                elif args_length == 1 and param.args[0] == "multi_update":
                    # special case for multi update from browser.
                    param.args.pop()
                    self._validateMultiRequests(param, safe, validate_request_update_args)
                elif args_length == 1 and param.args[0] == "bynames":
                    # special case for multi update from browser.
                    param.args.pop()
                    self._getRequestNamesFromBody(safe)
                else:
                    self._validateRequestBase(param, safe, validate_request_create_args)
        except cherrypy.HTTPError as exc:
            raise exc from None
        except InvalidSpecParameterValue as ex:
            raise ex from None
        except Exception as ex:
            # TODO add proper error message instead of trace back
            msg = traceback.format_exc()
            cherrypy.log("Error: %s" % msg)
            if hasattr(ex, "message"):
                if hasattr(ex.message, '__call__'):
                    msg = ex.message()
                else:
                    msg = str(ex)
            else:
                msg = str(ex)
            raise InvalidSpecParameterValue(msg) from None

    def _maskResult(self, mask, result):
        """
        If a mask of parameters was provided in the query string, then filter
        the request key/values accordingly.
        :param mask: a list of strings (keys of the request dictionary)
        :param result: a dict key'ed by the request name, with the whole
            request dictionary as a value
        :return: updates the result object in place and returns it (dict)
        """

        if len(mask) == 1 and mask[0] == "DAS":
            mask = ReqMgrConfigDataCache.getConfig("DAS_RESULT_FILTER")["filter_list"]

        if len(mask) > 0:
            maskedResult = {}
            for reqName, reqDict in viewitems(result):
                reqInfo = RequestInfo(reqDict)
                maskedResult.setdefault(reqName, {})
                for maskKey in mask:
                    foundValue = reqInfo.get(maskKey, None)
                    maskedResult[reqName].update({maskKey: foundValue})

            return maskedResult
        else:
            return result

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    def get(self, **kwargs):
        """
        Returns request info depending on the conditions set by kwargs
        Currently defined kwargs are following.
        statusList, requestNames, requestType, prepID, inputDataset, outputDataset, dateRange
        If jobInfo is True, returns jobInfomation about the request as well.

        TODO:
        stuff like this has to masked out from result of this call:
            _attachments: {u'spec': {u'stub': True, u'length': 51712, u'revpos': 2, u'content_type': u'application/json'}}
            _id: maxa_RequestString-OVERRIDE-ME_130621_174227_9225
            _rev: 4-c6ceb2737793aaeac3f1cdf591593da4

        """
        ### pop arguments unrelated to the user query
        mask = kwargs.pop("mask", [])
        detail = kwargs.pop("detail", True)
        common_dict = int(kwargs.pop("common_dict", 0))  # modifies the response format
        nostale = kwargs.pop("_nostale", False)

        ### these are the query strings supported by this API
        status = kwargs.get("status", [])
        name = kwargs.get("name", [])
        request_type = kwargs.get("request_type", [])
        prep_id = kwargs.get("prep_id", [])
        inputdataset = kwargs.get("inputdataset", [])
        outputdataset = kwargs.get("outputdataset", [])
        date_range = kwargs.get("date_range", False)
        campaign = kwargs.get("campaign", [])
        team = kwargs.get("team", [])
        mc_pileup = kwargs.get("mc_pileup", [])
        data_pileup = kwargs.get("data_pileup", [])
        requestor = kwargs.get("requestor", [])

        # further tweaks to the couch queries
        if len(status) == 1 and status[0] == "ACTIVE":
            status = ACTIVE_STATUS
        if detail in (False, "false", "False", "FALSE"):
            option = {"include_docs": False}
        else:
            option = {"include_docs": True}
        # everything should be stale view. this only needs for test
        if nostale:
            self.reqmgr_db_service._setNoStale()

        request_info = []
        queryMatched = False  # flag to avoid calling the same view twice
        if len(kwargs) == 2:
            if status and team:
                query_keys = [[t, s] for t in team for s in status]
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("byteamandstatus",
                                                                                 option, query_keys))
                queryMatched = True
            elif status and request_type:
                query_keys = [[s, rt] for rt in request_type for s in status]
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("requestsbystatusandtype",
                                                                                 option, query_keys))
                queryMatched = True
            elif status and requestor:
                query_keys = [[s, r] for r in requestor for s in status]
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bystatusandrequestor",
                                                                                 option, query_keys))
                queryMatched = True
        elif len(kwargs) == 3:
            if status and request_type and requestor:
                query_keys = [[s, rt, req] for s in status for rt in request_type for req in requestor]
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bystatusandtypeandrequestor",
                                                                                 option, query_keys))
                queryMatched = True

        # anything else that hasn't matched the query combination above
        if not queryMatched:
            if status:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bystatus",
                                                                                 option, status))
            if name:
                request_info.append(self.reqmgr_db_service.getRequestByNames(name))
            if request_type:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bytype",
                                                                                 option, request_type))
            if prep_id:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("byprepid",
                                                                                 option, prep_id))
            if inputdataset:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("byinputdataset",
                                                                                 option, inputdataset))
            if outputdataset:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("byoutputdataset",
                                                                                 option, outputdataset))
            if date_range:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bydate",
                                                                                 option, date_range))
            if campaign:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bycampaign",
                                                                                 option, campaign))
            if mc_pileup:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bymcpileup",
                                                                                 option, mc_pileup))
            if data_pileup:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bydatapileup",
                                                                                 option, data_pileup))

        # get the intersection of the request data
        result = self._intersection_of_request_info(request_info)

        if not result:
            return []

        result = self._maskResult(mask, result)

        if not option["include_docs"]:
            return list(result)

        # set the return format. default format has request name as a key
        # if is set to one it returns list of dictionary with RequestName field.
        if common_dict == 1:
            response_list = listvalues(result)
        else:
            response_list = [result]
        return rows(response_list)

    def _intersection_of_request_info(self, request_info):
        requests = {}
        if len(request_info) < 1:
            return requests

        request_key_set = set(request_info[0].keys())
        for info in request_info:
            request_key_set = set(request_key_set) & set(info.keys())
        # TODO: need to assume some data maight not contains include docs
        for request_name in request_key_set:
            requests[request_name] = request_info[0][request_name]
        return requests

    def _retrieveResubmissionChildren(self, request_name):
        """
        Fetches all the direct children requests from CouchDB.
        Response from CouchDB view is in the following format:
            [{u'id': u'child_workflow_name',
              u'key': u'parent_workflow_name',
              u'value': 'current_request_status'}]
        :param request_name: string with the parent workflow name
        :return: a list of dictionaries with the parent and child workflow and the child status
        """
        result = self.reqmgr_db.loadView('ReqMgr', 'childresubmissionrequests', keys=[request_name])['rows']
        childrenRequestAndStatus = []
        for childInfo in result:
            childrenRequestAndStatus.append(childInfo)
            childrenRequestAndStatus.extend(self._retrieveResubmissionChildren(childInfo['id']))
        return childrenRequestAndStatus

    def _handleNoStatusUpdate(self, workload, request_args, dn):
        """
        For no-status update, we only support the following parameters:
         1. RequestPriority
         2. Global workqueue statistics, while acquiring a workflow
        """
        if 'RequestPriority' in request_args:
            # Yes, we completely ignore any other arguments posted by the user (web UI case)
            request_args = {'RequestPriority': request_args['RequestPriority']}
            validate_request_priority(request_args)
            # must update three places: GQ elements, workload_cache and workload spec
            self.gq_service.updatePriority(workload.name(), request_args['RequestPriority'])
            report = self.reqmgr_db_service.updateRequestProperty(workload.name(), request_args, dn)
            workload.setPriority(request_args['RequestPriority'])
            workload.saveCouchUrl(workload.specUrl())
            cherrypy.log('Updated priority of "{}" to: {}'.format(workload.name(), request_args['RequestPriority']))
        elif workqueue_stat_validation(request_args):
            report = self.reqmgr_db_service.updateRequestStats(workload.name(), request_args)
            cherrypy.log('Updated workqueue statistics of "{}", with:  {}'.format(workload.name(), request_args))
        else:
            msg = "There are invalid arguments for no-status update: %s" % request_args
            raise InvalidSpecParameterValue(msg)

        return report

    def _handleAssignmentApprovedTransition(self, workload, request_args, dn):
        """
        Allows only two arguments: RequestStatus and RequestPriority
        """
        if "RequestPriority" not in request_args:
            msg = "There are invalid arguments for assignment-approved transition: %s" % request_args
            raise InvalidSpecParameterValue(msg)

        validate_request_priority(request_args)
        report = self.reqmgr_db_service.updateRequestProperty(workload.name(), request_args, dn)
        return report

    def _handleAssignmentStateTransition(self, workload, request_args, dn):
        if ('SoftTimeout' in request_args) and ('GracePeriod' in request_args):
            request_args['HardTimeout'] = request_args['SoftTimeout'] + request_args['GracePeriod']

        # Only allow extra value update for assigned status
        cherrypy.log("Assign request %s, input args: %s ..." % (workload.name(), request_args))
        try:
            workload.updateArguments(request_args)
        except Exception as ex:
            msg = traceback.format_exc()
            cherrypy.log("Error for request args %s: %s" % (request_args, msg))
            raise InvalidSpecParameterValue(str(ex))

        # validate/update OutputDatasets after ProcessingString and AcquisionEra is updated
        request_args['OutputDatasets'] = workload.listOutputDatasets()
        validateOutputDatasets(request_args['OutputDatasets'], workload.getDbsUrl())

        # by default, it contains all unmerged LFNs (used by sites to protect the unmerged area)
        request_args['OutputModulesLFNBases'] = workload.listAllOutputModulesLFNBases()

        # Add parentage relation for step chain, task chain:
        chainMap = workload.getChainParentageSimpleMapping()
        if chainMap:
            request_args["ChainParentageMap"] = chainMap

        # save the spec first before update the reqmgr request status to prevent race condition
        # when workflow is pulled to GQ before site white list is updated
        workload.saveCouch(self.config.couch_host, self.config.couch_reqmgr_db)
        report = self.reqmgr_db_service.updateRequestProperty(workload.name(), request_args, dn)

        return report

    def _handleOnlyStateTransition(self, workload, request_args, dn):
        """
        It handles only the state transition.
        Special handling needed if a request is aborted or force completed.
        """
        # if we got here, then the main workflow has been already validated
        # and the status transition is allowed
        req_status = request_args["RequestStatus"]
        cascade = request_args.get("cascade", False)

        if req_status in ["aborted", "force-complete"]:
            # cancel the workflow first
            self.gq_service.cancelWorkflow(workload.name())

        # cascade option is only supported for these 3 statuses. If set, we need to
        # find all the children requests and perform the same status transition
        if req_status in ["rejected", "closed-out", "announced"] and cascade:
            childrenNamesAndStatus = self._retrieveResubmissionChildren(workload.name())
            msg = "Workflow {} has {} ".format(workload.name(), len(childrenNamesAndStatus))
            msg += "children workflows to have a status transition to: {}".format(req_status)
            cherrypy.log(msg)
            for childInfo in childrenNamesAndStatus:
                if check_allowed_transition(childInfo['value'], req_status):
                    cherrypy.log('Updating request status for {} to {}.'.format(childInfo['id'], req_status))
                    self.reqmgr_db_service.updateRequestStatus(childInfo['id'], req_status, dn)
                else:
                    msg = "Status transition from {} to {} ".format(childInfo['value'], req_status)
                    msg += "not allowed for workflow: {}, skipping it!".format(childInfo['id'])
                    cherrypy.log(msg)
        # then update the original/parent workflow status in couchdb
        cherrypy.log('Updating request status for {} to {}.'.format(workload.name(), req_status))
        report = self.reqmgr_db_service.updateRequestStatus(workload.name(), req_status, dn)
        return report

    def _updateRequest(self, workload, request_args):
        dn = get_user_info().get("dn", "unknown")

        if "RequestStatus" not in request_args:
            report = self._handleNoStatusUpdate(workload, request_args, dn)
        else:
            req_status = request_args["RequestStatus"]
            if len(request_args) == 2 and req_status == "assignment-approved":
                report = self._handleAssignmentApprovedTransition(workload, request_args, dn)
            elif len(request_args) > 1 and req_status == "assigned":
                report = self._handleAssignmentStateTransition(workload, request_args, dn)
            elif len(request_args) == 1 or (len(request_args) == 2 and "cascade" in request_args):
                report = self._handleOnlyStateTransition(workload, request_args, dn)
            else:
                msg = "There are invalid arguments with this status transition: %s" % request_args
                raise InvalidSpecParameterValue(msg)

        if report == 'OK':
            return {workload.name(): "OK"}
        return {workload.name(): "ERROR"}

    @restcall(formats=[('application/json', JSONFormat())])
    def put(self, workload_pair_list):
        """workloadPairList is a list of tuple containing (workload, request_args)"""
        report = []
        for workload, request_args in workload_pair_list:
            result = self._updateRequest(workload, request_args)
            report.append(result)
        return report

    @restcall(formats=[('application/json', JSONFormat())])
    def delete(self, request_name):
        cherrypy.log("INFO: Deleting request document '%s' ..." % request_name)
        try:
            self.reqmgr_db.delete_doc(request_name)
        except CouchError as ex:
            msg = "ERROR: Delete failed."
            cherrypy.log(msg + " Reason: %s" % ex)
            raise cherrypy.HTTPError(404, msg)
            # TODO
        # delete should also happen on WMStats
        cherrypy.log("INFO: Delete '%s' done." % request_name)

    def _update_additional_request_args(self, workload, request_args):
        """
        add to request_args properties which is not initially set from user.
        This data will put in to couchdb.
        Update request_args here if additional information need to be put in couchdb
        """
        request_args['RequestWorkflow'] = sanitizeURL("%s/%s/%s/spec" % (request_args["CouchURL"],
                                                                         request_args["CouchWorkloadDBName"],
                                                                         workload.name()))['url']

        # Add the output datasets if necessary
        # for some bizarre reason OutpuDatasets is list of lists
        request_args['OutputDatasets'] = workload.listOutputDatasets()

        # Add initial priority only for the creation of the request
        request_args['InitialPriority'] = request_args["RequestPriority"]

        return

    @restcall(formats=[('application/json', JSONFormat())])
    def post(self, workload_pair_list, multi_update_flag=False, multi_names_flag=False):
        """
        Create and update couchDB with  a new request.
        request argument is passed from validation
        (validation convert cherrypy.request.body data to argument)

        TODO:
        this method will have some parts factored out so that e.g. clone call
        can share functionality.

        NOTES:
        1) do not strip spaces, #4705 will fails upon injection with spaces;
            currently the chain relies on a number of things coming in #4705
        2) reqInputArgs = Utilities.unidecode(json.loads(body))
            (from ReqMgrRESTModel.putRequest)
        """

        # storing the request document into Couch

        if multi_update_flag:
            return self.put(workload_pair_list)
        if multi_names_flag:
            return self.get(name=workload_pair_list)

        out = []
        for workload, request_args in workload_pair_list:
            self._update_additional_request_args(workload, request_args)

            cherrypy.log("Create request, input args: %s ..." % request_args)
            try:
                workload.saveCouch(request_args["CouchURL"], request_args["CouchWorkloadDBName"],
                                   metadata=request_args)
                out.append({'request': workload.name()})
            except Exception as ex:
                # then it failed to add the spec file as attachment
                # we better delete the original request to avoid confusion in wmstats
                cherrypy.log("Error saving request spec to couch: %s " % str(ex))
                self.delete(request_args['RequestName'])

        return out


class RequestStatus(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        validate_str("transition", param, safe, rx.RX_BOOL_FLAG, optional=True)
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["transiton"] = param.args[0]
            param.args.pop()
            return

    @restcall(formats=[('application/json', JSONFormat())])
    def get(self, transition):
        """
        Return list of allowed request status.
        If transition, return exhaustive list with all request status
        and their defined transitions.
        """
        if transition == "true":
            return rows([REQUEST_STATE_TRANSITION])
        return rows(REQUEST_STATE_LIST)


class RequestType(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        pass

    @restcall(formats=[('application/json', JSONFormat())])
    def get(self):
        return rows(REQUEST_TYPES)
