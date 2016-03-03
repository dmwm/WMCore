"""
ReqMgr request handling.

"""

import cherrypy
import traceback

from WMCore.Lexicon import sanitizeURL
from WMCore.Database.CMSCouch import CouchError
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.Wrappers import JsonWrapper

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Validation import validate_str
from WMCore.REST.Format import JSONFormat

import WMCore.ReqMgr.Service.RegExp as rx
from WMCore.ReqMgr.DataStructs.Request import initialize_request_args
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_LIST
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATE_TRANSITION
from WMCore.ReqMgr.DataStructs.RequestType import REQUEST_TYPES
from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue
from WMCore.ReqMgr.Utils.Validation import validate_request_create_args, \
    validate_request_update_args, loadRequestSchema

from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
    
class Request(RESTEntity):
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_db = api.db_handler.get_db(config.couch_reqmgr_db)
        self.reqmgr_db_service = RequestDBWriter(self.reqmgr_db, couchapp="ReqMgr")
        # this need for the post validtiaon
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)
        self.gq_service = WorkQueue(config.couch_host, config.couch_workqueue_db)

    def _requestArgMapFromBrowser(self, request_args):
        """
        This is specific mapping function data from browser

        TODO: give a key word so it doesn't have to loop though in general
        """
        docs = []
        for doc in request_args:
            for key in doc.keys():
                if key.startswith('request'):
                    rid = key.split('request-')[-1]
                    if rid != 'all':
                        docs.append(rid)
                    del doc[key]
        return docs

    def _validateGET(self, param, safe):
        # TODO: need proper validation but for now pass everything
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["name"] = param.args[0]
            param.args.pop()
            return

        if "status" in param.kwargs and isinstance(param.kwargs["status"], basestring):
            param.kwargs["status"] = [param.kwargs["status"]]
        if "status" in param.kwargs:
            for status in param.kwargs["status"]:
                if status.endswith("-archived"):
                    raise InvalidSpecParameterValue(
                        "Can't retrieve bulk archived status requests, use other search arguments")

        for prop in param.kwargs:
            safe.kwargs[prop] = param.kwargs[prop]

        for prop in safe.kwargs:
            del param.kwargs[prop]

        return

    def _validateRequestBase(self, param, safe, valFunc, requestName=None):

        data = cherrypy.request.body.read()
        if data:
            request_args = JsonWrapper.loads(data)
            if requestName:
                request_args["RequestName"] = requestName
            if isinstance(request_args, dict):
                request_args = [request_args]

        else:
            # actually this is error case
            # cherrypy.log(str(param.kwargs))
            request_args = {}
            for prop in param.kwargs:
                request_args[prop] = param.kwargs[prop]

            for prop in request_args:
                del param.kwargs[prop]
            if requestName:
                request_args["RequestName"] = requestName
            request_args = [request_args]

        safe.kwargs['workload_pair_list'] = []
        if isinstance(request_args, dict):
            request_args = [request_args]
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
        elif isinstance(ids, basestring):
            doc[ids] = 'on'

        docs = []
        for key in doc.keys():
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
            request_names, request_args = self._getMultiRequestArgs(JsonWrapper.loads(data))
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

            # remove this
            # tmp = []
            # for prop in param.kwargs:
            #    tmp.append(prop)
            # for prop in tmp:
            #    del param.kwargs[prop]

        safe.kwargs['workload_pair_list'] = []

        for request_name in request_names:
            request_args["RequestName"] = request_name
            workload, r_args = valFunc(request_args, self.config, self.reqmgr_db_service, param)
            safe.kwargs['workload_pair_list'].append((workload, r_args))

        safe.kwargs["multi_update_flag"] = True

    def _getRequestNamesFromBody(self, param, safe, valFunc):

        request_names = JsonWrapper.loads(cherrypy.request.body.read())
        safe.kwargs['workload_pair_list'] = request_names
        safe.kwargs["multi_names_flag"] = True

    def validate(self, apiobj, method, api, param, safe):
        # to make validate successful
        # move the validated argument to safe
        # make param empty
        # other wise raise the error
        try:
            if method in ['GET']:
                self._validateGET(param, safe)

            if method == 'PUT':
                args_length = len(param.args)
                if args_length == 1:
                    requestName = param.args[0]
                    param.args.pop()
                else:
                    requestName = None
                self._validateRequestBase(param, safe, validate_request_update_args, requestName)
                # TO: handle multiple clone
            #                 if len(param.args) == 2:
            #                     #validate clone case
            #                     if param.args[0] == "clone":
            #                         param.args.pop()
            #                         return None, request_args

            if method == 'POST':
                args_length = len(param.args)
                if args_length == 1 and param.args[0] == "multi_update":
                    # special case for multi update from browser.
                    param.args.pop()
                    self._validateMultiRequests(param, safe, validate_request_update_args)
                elif args_length == 1 and param.args[0] == "bynames":
                    # special case for multi update from browser.
                    param.args.pop()
                    self._getRequestNamesFromBody(param, safe, validate_request_update_args)
                else:
                    self._validateRequestBase(param, safe, validate_request_create_args)
        except InvalidSpecParameterValue as ex:
            raise ex
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
            raise InvalidSpecParameterValue(msg)

    def initialize_clone(self, request_name):
        requests = self.reqmgr_db_service.getRequestByNames(request_name)
        clone_args = requests.values()[0]
        # overwrite the name and time stamp.
        initialize_request_args(clone_args, self.config, clone=True)
        # timestamp status update

        spec = loadSpecByType(clone_args["RequestType"])
        workload = spec.factoryWorkloadConstruction(clone_args["RequestName"],
                                                    clone_args)
        return (workload, clone_args)

    @restcall(formats=[('application/json', JSONFormat())])
    def get(self, **kwargs):
        """
        Returns request info depending on the conditions set by kwargs
        Currently defined kwargs are following.
        statusList, requestNames, requestType, prepID, inputDataset, outputDataset, dateRange
        If jobInfo is True, returns jobInfomation about the request as well.

        TODO:
        stuff like this has to filtered out from result of this call:
            _attachments: {u'spec': {u'stub': True, u'length': 51712, u'revpos': 2, u'content_type': u'application/json'}}
            _id: maxa_RequestString-OVERRIDE-ME_130621_174227_9225
            _rev: 4-c6ceb2737793aaeac3f1cdf591593da4

        """
        if len(kwargs) == 0:
            kwargs['status'] = "running"
            options = {"descending": True, 'include_docs': True, 'limit': 200}
            request_docs = self.reqmgr_db.loadView("ReqMgr", "bystatus", options)
            return rows([request_docs])

        # list of status
        status = kwargs.get("status", [])
        # list of request names
        name = kwargs.get("name", False)
        request_type = kwargs.get("request_type", False)
        prep_id = kwargs.get("prep_id", False)
        inputdataset = kwargs.get("inputdataset", False)
        outputdataset = kwargs.get("outputdataset", False)
        date_range = kwargs.get("date_range", False)
        campaign = kwargs.get("campaign", False)
        workqueue = kwargs.get("workqueue", False)
        team = kwargs.get("team", False)
        mc_pileup = kwargs.get("mc_pileup", False)
        data_pileup = kwargs.get("data_pileup", False)
        detail = kwargs.get("detail", True)
        if detail in (False, "false", "False"):
            option = {"include_docs": False}
        else:
            option = {"include_docs": True}
        # eventhing should be stale view. this only needs for test
        _nostale = kwargs.get("_nostale", False)
        if _nostale:
            self.reqmgr_db_service._setNoStale()

        request_info = []

        for st in status:
            if not team and not request_type:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("bystatus", option, st))
            elif team:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("byteamandstatus", option, [[team, st]]))
            elif request_type:
                request_info.append(self.reqmgr_db_service.getRequestByCouchView("requestsbystatusandtype",
                                                                                 option, [[st, request_type]]))
        if name:
            request_info.append(self.reqmgr_db_service.getRequestByNames(name))
        if prep_id:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("byprepid", option, prep_id))
        if inputdataset:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("byinputdataset", option, inputdataset))
        if outputdataset:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("byoutputdataset", option, outputdataset))
        if date_range:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("bydate", option, date_range))
        if campaign:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("bycampaign", option, campaign))
        if workqueue:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("byworkqueue", option, workqueue))
        if mc_pileup:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("bymcpileup", option, mc_pileup))
        if data_pileup:
            request_info.append(self.reqmgr_db_service.getRequestByCouchView("bydatapileup", option, data_pileup))
        # get interaction of the request
        result = self._intersection_of_request_info(request_info)
        if len(result) == 0:
            return []
        return rows([result])

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

        # TODO move this out of this class

    def filterCouchInfo(self, couchInfo):
        for key in ['_rev', '_attachments']:
            if key in couchInfo:
                del couchInfo[key]

    def _combine_request(self, request_info, requestAgentUrl, cache):
        keys = {}
        requestAgentUrlList = []
        for row in requestAgentUrl["rows"]:
            request = row["key"][0]
            if not keys[request]:
                keys[request] = []
            keys[request].append(row["key"][1])

        for request in request_info:
            for agentUrl in keys[request]:
                requestAgentUrlList.append([request, agentUrl])

        return requestAgentUrlList

    def _retrieveResubmissionChildren(self, request_name):

        result = self.reqmgr_db.loadView('ReqMgr', 'childresubmissionrequests', keys=[request_name])['rows']
        childrenRequestNames = []
        for child in result:
            childrenRequestNames.append(child['id'])
            childrenRequestNames.extend(self._retrieveResubmissionChildren(child['id']))
        return childrenRequestNames

    def _updateRequest(self, workload, request_args):

        if workload == None:
            (workload, request_args) = self.initialize_clone(request_args["OriginalRequestName"])
            return self.post([workload, request_args])

        dn = cherrypy.request.user.get("dn", "unknown")

        # allow Priority update any time.
        if 'RequestPriority' in request_args: 
            self.gq_service.updatePriority(workload.name(), int(request_args['RequestPriority']))

        if "total_jobs" in request_args:
            # only GQ update this stats
            # request_args should contain only 4 keys 'total_jobs', 'input_lumis', 'input_events', 'input_num_files'}
            report = self.reqmgr_db_service.updateRequestStats(workload.name(), request_args)
            
        if ('SoftTimeout' in request_args) and ('GracePeriod' in request_args):
            request_args['HardTimeout'] = int(request_args['SoftTimeout']) + int(request_args['GracePeriod'])
        
        
        req_status = request_args.get("RequestStatus", None)

        if (len(request_args) >= 1 and req_status == None) or \
            (len(request_args) > 1 and req_status in ["assigned", "assignment-approved"]):
            #Only allow extra value update for assigned status
            try:
                workload.updateArguments(request_args)
            except Exception as ex:
                msg = traceback.format_exc()
                cherrypy.log("Error for request args %s: %s" % (request_args, msg))
                raise InvalidSpecParameterValue(str(ex))
            
            # legacy update schema to support ops script
            loadRequestSchema(workload, request_args)
            # trailing / is needed for the savecouchUrl function
            report = self.reqmgr_db_service.updateRequestProperty(workload.name(), request_args, dn)
            workload.saveCouch(self.config.couch_host, self.config.couch_reqmgr_db)
        
        elif len(request_args) == 2 and req_status in ["closed-out", "announced"] and \
             request_args.get("cascade", False):
            # check whehter it is casecade option
            cascade_list = self._retrieveResubmissionChildren(workload.name())
            for req_name in cascade_list:
                report = self.reqmgr_db_service.updateRequestStatus(req_name, req_status, dn)
            # update original workflow status
            report = self.reqmgr_db_service.updateRequestStatus(workload.name(), req_status, dn)
            
        elif len(request_args) == 1 and req_status != None:
            # incase only the state transition happens
            if req_status == "aborted" or req_status == "force-complete":
                # cancel the workflow first
                self.gq_service.cancelWorkflow(workload.name())
            #update the request status in couchdb    
            report = self.reqmgr_db_service.updateRequestStatus(workload.name(), req_status, dn)
        else:
            raise InvalidSpecParameterValue(
                        "can't update value except transition to assigned status: %s" % request_args)

        if report == 'OK':
            return {workload.name(): "OK"}
        else:
            return {workload.name(): "ERROR"}

    @restcall(formats=[('application/json', JSONFormat())])
    def put(self, workload_pair_list):
        "workloadPairList is a list of tuple containing (workload, requeat_args)"
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
        
        #Add initial priority only for the creation of the request
        request_args['InitialPriority'] = request_args["RequestPriority"]
         
        # TODO: remove this after reqmgr2 replice reqmgr (reqmgr2Only)
        request_args['ReqMgr2Only'] = True
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
        2) reqInputArgs = Utilities.unidecode(JsonWrapper.loads(body))
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
            
            # legacy update schema to support ops script
            loadRequestSchema(workload, request_args)
            
            cherrypy.log("INFO: Create request, input args: %s ..." % request_args)
            workload.saveCouch(request_args["CouchURL"], request_args["CouchWorkloadDBName"],
                               metadata=request_args)
            out.append({'request': workload.name()})
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
        else:
            return rows(REQUEST_STATE_LIST)


class RequestType(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)

    def validate(self, apiobj, method, api, param, safe):
        pass

    @restcall
    def get(self):
        return rows(REQUEST_TYPES)
