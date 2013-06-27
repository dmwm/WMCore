"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.Database.CMSCouch import Database, CouchError
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import WMSpecFactoryException
from WMCore.Wrappers import JsonWrapper
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str

from WMCore.ReqMgr.Service.Auxiliary import ReqMgrBaseRestEntity
import WMCore.ReqMgr.Service.RegExp as rx

from WMCore.ReqMgr.DataStructs.Request import RequestDataError
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATUS_LIST
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATUS_TRANSITION
from WMCore.ReqMgr.DataStructs.RequestType import REQUEST_TYPES
from WMCore.ReqMgr.DataStructs.Request import Request as RequestData



class Request(ReqMgrBaseRestEntity):
    def __init__(self, app, api, config, mount, db_handler):
        # main CouchDB database where requests/workloads are stored
        self.db_name = config.couch_reqmgr_db
        ReqMgrBaseRestEntity.__init__(self, app, api, config, mount, db_handler)

        
    def validate(self, apiobj, method, api, param, safe):
        if method in ("GET",):
            validate_str("all", param, safe, rx.RX_BOOL_FLAG, optional=True)
            validate_str("request_name", param, safe, rx.RX_REQUEST_NAME, optional=True)
        if method in ("DELETE", ):
            validate_str("request_name", param, safe, rx.RX_REQUEST_NAME, optional=False)
        
    
    @restcall
    def get(self, request_name, all):
        """
        Returns most recent list of requests in the system.
        Query particular request if request_name is specified.
        Return complete list of all requests in the system if all is set.
            If all is not set, check "default_view_requests_since_num_days"
            config value and show only requests not older than this
            number of days.
            
        TODO:
        stuff like this has to filtered out from result of this call:
            _attachments: {u'spec': {u'stub': True, u'length': 51712, u'revpos': 2, u'content_type': u'application/json'}}
            _id: maxa_RequestString-OVERRIDE-ME_130621_174227_9225
            _rev: 4-c6ceb2737793aaeac3f1cdf591593da4
        
        """
        if request_name:
            request_doc = self.db_handler.document(self.db_name, request_name)
            return rows([request_doc])
        else:
            options = {"descending": True}
            if all == "false":
                past_days = self.config.default_view_requests_since_num_days
                current_date = list(time.gmtime()[:6])
                from_date = datetime(*current_date) - timedelta(days=past_days)
                options["endkey"] = list(from_date.timetuple()[:6])
            request_docs = self.db_handler.view(self.db_name,
                                                "ReqMgr", "bydate",
                                                options=options)
            return rows([request_docs])
        
        
    @restcall
    def delete(self, request_name):
        cherrypy.log("INFO: Deleting request document '%s' ..." % request_name)
        couchdb = self.db_handler.get_db(self.db_name)
        try:
            couchdb.delete_doc(request_name)
        except CouchError, ex:
            msg = "ERROR: Delete failed."
            cherrypy.log(msg + " Reason: %s" % ex)
            raise cherrypy.HTTPError(404, msg)        
        # TODO
        # delete should also happen on WMStats
        cherrypy.log("INFO: Delete '%s' done." % request_name)
        
    
    @restcall
    def post(self):
        """
        Create / inject a new request. Request input schema is specified in 
        the body of the request as JSON encoded data.
        
        All request arguments manipulation to happen in the .request_initialize()
        
        All request arguments validation to happen in DataStructs.Request.validate() 
        
        TODO:
        this method will have some parts factored out so that e.g. clone call
        can share functionality.
        
        """
        json_input_request_args = cherrypy.request.body.read()
        request_input_dict = JsonWrapper.loads(json_input_request_args)        
        # strip spaces
        for k, v in request_input_dict.iteritems():
            if isinstance(v, str):
                request_input_dict[k] = v.strip()
        
        cherrypy.log("INFO: Create request, input args: %s ..." % request_input_dict)
        
        # TODO
        # necessary? (is in ReqMgrRESTModel.putRequest)
        #reqInputArgs = Utilities.unidecode()
        
        request = RequestData() # this returns a new request dictionary
        request.update(request_input_dict)

        try:        
            request.validate_automatic_args_empty()
            # fill in automatic request arguments and further request args meddling
            self.request_initialize(request)
            request.validate()
        except RequestDataError, ex:
            cherrypy.log(ex.message)
            raise cherrypy.HTTPError(400, ex.message)
                
        # the request arguments below shall be handled automatically by either
        # being specified in the input or already have correct default
        # values in the schema definition
        
        # the arguments manipulation stuff (currently commented out) below
        # should all go into request request_initialize()

        # TODO
        # do we need InputDataset and InputDatasets?
        # this likely the cause of #3743
        #if reqInputArgs.has_key("InputDataset"):
        # reqSchema["InputDatasets"] = [reqInputArgs["InputDataset"]]

        # TODO
        # don't understand this, have to clarify with Diego
        # it's from (ReqMgrWebtools) Utilities.makeRequest()
        """
        skimNumber = 1
        # a list of dictionaries
        reqSchema["SkimConfigs"] = []
        while reqInputArgs.has_key("SkimName%s" % skimNumber):
            d = {}
            d["SkimName"] = reqInputArgs["SkimName%s" % skimNumber]
            d["SkimInput"] = reqInputArgs["SkimInput%s" % skimNumber]
            d["Scenario"] = reqInputArgs["Scenario"]
            d["TimePerEvent"] = reqInputArgs.get("SkimTimePerEvent%s" % skimNumber, None)
            d["SizePerEvent"] = reqInputArgs.get("SkimSizePerEvent%s" % skimNumber, None)
            d["Memory"] = reqInputArgs.get("SkimMemory%s" % skimNumber, None)
    
    
            if reqInputArgs.get("Skim%sConfigCacheID" % skimNumber, None) != None:
                d["ConfigCacheID"] = reqInputArgs["Skim%sConfigCacheID" % skimNumber]
    
            reqSchema["SkimConfigs"].append(d)
            skimNumber += 1
        """
        
        # TODO
        # another arguments fiddling mess, do later
        # it's from (ReqMgrWebtools) Utilities.makeRequest()
        """
        if reqInputArgs.has_key("DataPileup") or reqInputArgs.has_key("MCPileup"):
            reqSchema["PileupConfig"] = {}
            if reqInputArgs.has_key("DataPileup") and reqInputArgs["DataPileup"] != "":
                reqSchema["PileupConfig"]["data"] = [reqInputArgs["DataPileup"]]
            if reqInputArgs.has_key("MCPileup") and reqInputArgs["MCPileup"] != "":
                reqSchema["PileupConfig"]["mc"] = [reqInputArgs["MCPileup"]]
        """
        
        # TODO
        # it's from (ReqMgrWebtools) Utilities.makeRequest()
        # should go into request_initialize()
        """
        for runlist in ["RunWhitelist", "RunBlacklist"]:
            if runlist in reqInputArgs:
                reqSchema[runlist] = parseRunList(reqInputArgs[runlist])
        for blocklist in ["BlockWhitelist", "BlockBlacklist"]:
            if blocklist in reqInputArgs:
                reqSchema[blocklist] = parseBlockList(reqInputArgs[blocklist])
        for stringList in ["DqmSequences", "IgnoredOutputModules", "TransientOutputModules"]:
            if stringList in reqInputArgs:
                reqSchema[stringList] = parseStringListWithoutValidation(reqInputArgs[stringList])
        """
        
        cherrypy.log("INFO: Request initialization and validation succeeded.")
        
        # it's from (ReqMgrWebtools) Utilities.makeRequest()
        # request = buildWorkloadAndCheckIn(webApi, reqSchema, couchUrl, couchDB, wmstatUrl)

        # build workload
        # WMCore/RequestManager/RequestMaker/Registry.py
        try:
            factory_name = "%sWorkloadFactory" % request["RequestType"]
            mod = __import__("WMCore.WMSpec.StdSpecs.%s" % request["RequestType"],
                             globals(), locals(), [factory_name])
            Factory = getattr(mod, factory_name)
        except ImportError:
            msg =  "ERROR: Spec type '%s' not found in WMCore.WMSpec.StdSpecs" % request["RequestType"]
            cherrypy.log(msg)
            raise RuntimeError, msg
        except AttributeError, ex:
            msg = "ERROR: Factory not found in Spec for type '%s'" % request["RequestType"]
            cherrypy.log(msg)
            raise RuntimeError, msg

        try:
            factory = Factory()
            # TODO
            # this method is only used by ReqMgr1, once ReqMgr1 is gone,
            # there can be only 1 argument to this method
            workload = factory.factoryWorkloadConstruction(workloadName=request["RequestName"],
                                                           arguments=request)
            self.request_initilize_attach_input_to_workload(workload, request_input_dict)        
        except WMSpecFactoryException, ex:
            msg = "ERROR: Error in spec/workload validation: %s" % ex._message
            cherrypy.log(msg)
            raise cherrypy.HTTPError(400, msg)
        
        # TODO
        # watch the above instantiation, seems to take rather long time to pass
        
        cherrypy.log("INFO: Request corresponding workload instantiated, storing ...")
            
        # TODO
        # explore what exactly is here in the data
        # is it the spec instance?, then it all fine
        request["WorkloadSpec"] = workload.data
        
        if request.get("DbsUrl", None):
            request["DbsUrl"] = (workload.getTopLevelTask()[0]).dbsUrl()
                
        # workload under request["WorkloadSpec"] is now part of the request instance
        # at this point, it's the end of 
        # Registry.buildWorkloadForRequest(typename, schema):
        
        helper = WMWorkloadHelper(request["WorkloadSpec"])

        # TODO
        # this should be revised for ReqMgr2        
        #4378 - ACDC (Resubmission) requests should inherit the Campaign ...
        # for Resubmission request, there already is previous Campaign set
        # this call would override it with initial request arguments where
        # it is not specified, so would become ''
        # TODO
        # these kind of calls should go into some workload initialization
        if not helper.getCampaign():
            helper.setCampaign(request["Campaign"])

        if request.has_key("RunWhitelist"):
            helper.setRunWhitelist(request["RunWhitelist"])
        
        # storing the request document into Couch

        # can't save Request object directly, because it makes it hard to retrieve
        # the _rev
        # TODO
        # don't understand this. may just be possible to keep dealing with
        # 'request' and not create this metadata
        metadata = {}
        metadata.update(request)    
    
        # Add the output datasets if necessary
        # for some bizarre reason OutpuDatasets is list of lists, when cloning
        # [['/MinimumBias/WMAgentCommissioning10-v2/RECO'], ['/MinimumBias/WMAgentCommissioning10-v2/ALCARECO']]
        # #3743
        # TODO
        # this should be verified and straighten up in ReqMgr2, should not need this
        #if not clone:
        #    for ds in helper.listOutputDatasets():
        #        if ds not in request['OutputDatasets']:
        #            request['OutputDatasets'].append(ds)
                
        # Store new request into Couch
        
        try:
            # don't want to JSONify the whole workflow
            del metadata["WorkloadSpec"]
            workload_url = helper.saveCouch(request["CouchURL"],
                                            request["CouchWorkloadDBName"],
                                            metadata=metadata)
            
            # TODO
            # this will have to be updated now, when the Couch url is known. The question
            # is whether this request argument is necessary since it should always be
            # CouchUrl/DbName/RequestName/spec so it can easily be derived 
            request["RequestWorkflow"] = workload_url
        
            # now comes the step of storing into Oracle
            # CheckIn.checkIn(request, reqSchema['RequestType'])
    
            # TODO
            # was the case in ReqMgr1, now just update RequestWorkflow, it's the URL
            # and it's not entire clear if this is necessary since it can be worked out
            # otherwise, have to check
            # this will most likely not be necessary
            params_to_update = ["RequestWorkflow"]
            couchdb = Database(request["CouchWorkloadDBName"], request["CouchURL"])
            fields = {}
            for key in params_to_update:
                fields[key] = request[key]
            couchdb.updateDocument(request["RequestName"], "ReqMgr", "updaterequest",
                                   fields=fields)
        except CouchError, ex:
            # TODO simulate exception here to see how much gets exposed to the client
            # and how much gets logged when it's like this
            msg = "ERROR: Storing into Couch failed, reason: %s" % ex.reason
            cherrypy.log(msg)
            raise cherrypy.HTTPError(500, msg)
             
        # TODO
        # WMStats interaction
        # 1) first there should be a link to the database so it's not necessary
        #    to instantiate the writer it at every single manipulation
        # 2) scrutinize the request injection flow from ReqMgrRESTModel.putRequest()
        #    where and how WMStatsURL is used, concentrate stuff at 1 place
        # this is what ReqMgrWebTools.buildWorkloadAndCheckIn() has:
        """        
        try:
            wmstatSvc = WMStatsWriter(wmstatUrl)
            wmstatSvc.insertRequest(request)
        except Exception as ex:
            webApi.error("Could not update WMStats, reason: %s" % ex)
            raise HTTPError(400, "Creating request failed, could not update WMStats.")
        """

        cherrypy.log("INFO: Request '%s' created and stored." % request["RequestName"])        
        # do not want to return to client spec data
        del request["WorkloadSpec"]
        return rows([request])


    def request_initialize(self, request):
        """
        Request data class request is a dictionary representing
        a being injected / created request. This method initializes
        various request fields. This should be the ONLY method to
        manipulate request arguments upon injection so that various
        levels or arguments manipulation does not occur accros several
        modules and across about 7 various methods like in ReqMgr1.
        
        request is changed here.
        
        """ 
        request["CouchURL"] = self.config.couch_host
        request["CouchWorkloadDBName"] = self.config.couch_reqmgr_db
        request["CouchDBName"] = self.config.couch_config_cache_db
        request["Requestor"] = cherrypy.request.user["login"]
        request["RequestorDN"] = cherrypy.request.user.get("dn", "unknown")
        # assign first starting status, should be 'new'
        request["RequestStatus"] = REQUEST_STATUS_LIST[0]

        current_time = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
        seconds = int(10000 * (time.time() % 1.0))
        request_string = request.get("RequestString", "")
        if request_string != "":
            request["RequestName"] = "%s_%s" % (request["Requestor"], request_string)
        else:
            request["RequestName"] = request["Requestor"]
        request["RequestName"] += "_%s_%s" % (current_time, seconds)    
        request["RequestDate"] = list(time.gmtime()[:6])
        
        if request.has_key("FilterEfficiency"):
            request["FilterEfficiency"] = float(request["FilterEfficiency"])
        if request["CMSSWVersion"] and request["CMSSWVersion"] not in request["SoftwareVersions"]:
            request["SoftwareVersions"].append(request["CMSSWVersion"])
            
            
    def request_initilize_attach_input_to_workload(self, workload, request_input_dict):
        """
        request_input_dict are input arguments for request injection
        workload is a corresponding newly created workload instance and 
            request_input_dict is attached to workload under 'schema'
        
        ReqMgr1 does this in RequestMaker.Registry.loadRequestSchema()
        
        This method is only a slight modification of loadRequestSchema()
        
        Storing this original injection time information is probably not
        crucially necessary but is definitely practical, keep that.
        
        """
        wl = workload
        rid = request_input_dict
        schema = wl.data.request.section_("schema")
        for key, value in rid.iteritems():
            try:
                setattr(schema, key, value)
            except Exception, ex:
                # attach TaskChain tasks
                # TODO this may be a good example where recursion is practical
                if (type(value) == dict and rid["RequestType"] == 'TaskChain' and
                    "Task" in key):
                    new_section = schema.section_(key)
                    for k, v in rid[key].iteritems():
                        try:
                            setattr(new_section, k, v)
                        except Exception, ex:
                            # what does this mean then?
                            pass
                else:
                    pass
        schema.timeStamp = int(time.time())
        wl.data.owner.Group = schema.Group
        # TODO
        # ReqMgr2 does not allow Requestor request argument specified in the
        # injection input so it can't be set here based on request_input_dict.
        # If it's absolutely necessary, it can be passed to his method from the 
        # called, where it's known, and set on the workload. But I doubt there
        # needs to be so much data duplication yet again between stuff stored
        # in Couch and on workload.
        #wl.data.owner.Requestor = schema.Requestor
        if hasattr(schema, "RequestPriority"):
            wl.data.request.priority = schema.RequestPriority
  
        
        
class RequestStatus(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)


    def validate(self, apiobj, method, api, param, safe):
        validate_str("transition", param, safe, rx.RX_BOOL_FLAG, optional=True)
    
    
    @restcall
    def get(self, transition):
        """
        Return list of allowed request status.
        If transition, return exhaustive list with all request status
        and their defined transitions.
        
        """
        if transition == "true":
            return rows(REQUEST_STATUS_TRANSITION)
        else:
            return rows(REQUEST_STATUS_LIST)
    
    
    
class RequestType(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
    
    
    def validate(self, apiobj, method, api, param, safe):
        pass
    
    
    @restcall
    def get(self):
        return rows(REQUEST_TYPES)