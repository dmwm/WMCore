"""
ReqMgr request handling.

"""
import json
import time
import logging

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType, loadSpecClassByType, setArgumentsWithDefault
from WMCore.REST.Auth import authz_match
from WMCore.WMFactory import WMFactory
from WMCore.Services.DBS.DBS3Reader import DBS3Reader as DBSReader
from WMCore.ReqMgr.Auth import getWritePermission
from WMCore.ReqMgr.DataStructs.Request import initialize_request_args
from WMCore.ReqMgr.DataStructs.RequestStatus import check_allowed_transition, STATES_ALLOW_ONLY_STATE_TRANSITION
from WMCore.ReqMgr.DataStructs.RequestError import InvalidStateTransition, InvalidSpecParameterValue
from WMCore.ReqMgr.Tools.cms import releases, architectures, dashboardActivities
from WMCore.Lexicon import procdataset


def loadRequestSchema(workload, requestSchema):
    """
    _loadRequestSchema_
    Legacy code to support ops script
    
    Does modifications to the workload I don't understand
    Takes a WMWorkloadHelper, operates on it directly with the schema
    """
    schema = workload.data.request.section_('schema')
    for key, value in requestSchema.iteritems():
        if isinstance(value, dict) and key == 'LumiList':
            value = json.dumps(value)
        try:
            setattr(schema, key, value)
        except Exception as ex:
            # Attach TaskChain tasks
            if isinstance(value, dict) and requestSchema['RequestType'] == 'TaskChain' and 'Task' in key:
                newSec = schema.section_(key)
                for k, v in requestSchema[key].iteritems():
                    if isinstance(value, dict) and key == 'LumiList':
                        value = json.dumps(value)
                    try:
                        setattr(newSec, k, v)
                    except Exception as ex:
                        # this logging need to change to cherry py logging
                        logging.error("Invalid Value: %s" % str(ex))
            else:
                # this logging need to change to cherry py logging 
                logging.error("Invalid Value: %s" % str(ex))

    schema.timeStamp = int(time.time())
    schema = workload.data.request.schema

    # might belong in another method to apply existing schema
    workload.data.owner.Group = schema.Group
    workload.data.owner.Requestor = schema.Requestor


def workqueue_stat_validation(request_args):
    stat_keys = ['total_jobs', 'input_lumis', 'input_events', 'input_num_files']
    return set(request_args.keys()) == set(stat_keys)


def validate_request_update_args(request_args, config, reqmgr_db_service, param):
    """
    param and safe structure is RESTArgs structure: named tuple
    RESTArgs(args=[], kwargs={})
    
    validate post/put request
    1. read data from body
    2. validate the permission (authentication)
    3. validate state transition (against previous state from couchdb)
    2. validate using workload validation
    3. convert data from body to arguments (spec instance, argument with default setting)
    
    TODO: raise right kind of error with clear message
    """
    # this needs to be deleted for validation
    request_name = request_args.pop("RequestName")
    couchurl = '%s/%s' % (config.couch_host, config.couch_reqmgr_db)
    workload = WMWorkloadHelper()
    workload.loadSpecFromCouch(couchurl, request_name)

    # first validate the permission by status and request type.
    # if the status is not set only ReqMgr Admin can change the values
    # TODO for each step, assigned, approved, announce find out what other values
    # can be set
    request_args["RequestType"] = workload.requestType()
    permission = getWritePermission(request_args)
    authz_match(permission['role'], permission['group'])
    del request_args["RequestType"]

    # validate the status
    if "RequestStatus" in request_args:
        validate_state_transition(reqmgr_db_service, request_name, request_args["RequestStatus"])
        # delete request_args since it is not part of spec argument and validation
        if request_args["RequestStatus"] not in STATES_ALLOW_ONLY_STATE_TRANSITION:
            args_without_status = {}
            args_without_status.update(request_args)
            del args_without_status["RequestStatus"]
        else:
            #if state change doesn't allow other transition nothing else to validate
            args_only_status = {}
            args_only_status["RequestStatus"] = request_args["RequestStatus"]
            return  workload, args_only_status 
    else:
        args_without_status = request_args

    if len(args_without_status) == 1:
        if 'RequestPriority' in args_without_status:
            args_without_status['RequestPriority'] = int(args_without_status['RequestPriority'])
            if (lambda x: (x >= 0 and x < 1e6))(args_without_status['RequestPriority']) is False:
                raise InvalidSpecParameterValue("RequestPriority must be an integer between 0 and 1e6")
            return workload, args_without_status
        elif 'cascade' in args_without_status:
            # status was already validated
            return workload, request_args
    elif len(args_without_status) > 0 and not workqueue_stat_validation(args_without_status):
        # validate the arguments against the spec argumentSpecdefinition
        # TODO: currently only assigned status allows any update other then Status update
        workload.validateArgumentForAssignment(args_without_status)

    # to update request_args with type conversion
    request_args.update(args_without_status)
    return workload, request_args


def validate_request_create_args(request_args, config, *args, **kwargs):
    """
    *arg and **kwargs are only for the interface
    validate post request
    1. read data from body
    2. validate using spec validation
    3. convert data from body to arguments (spec instance, argument with default setting) 
    TODO: rasie right kind of error with clear message 
    """

    initialize_request_args(request_args, config)
    # check the permission for creating the request
    permission = getWritePermission(request_args)
    authz_match(permission['role'], permission['group'])

    # set default values for teh request_args
    specClass = loadSpecClassByType(request_args["RequestType"])
    setArgumentsWithDefault(request_args, specClass.getWorkloadArguments())

    # get the spec type and validate arguments
    spec = loadSpecByType(request_args["RequestType"])
    if request_args["RequestType"] == "Resubmission":
        request_args["OriginalRequestCouchURL"] = '%s/%s' % (config.couch_host,
                                                             config.couch_reqmgr_db)
    workload = spec.factoryWorkloadConstruction(request_args["RequestName"],
                                                request_args)
    return workload, request_args


def validate_state_transition(reqmgr_db_service, request_name, new_state):
    """
    validate state transition by getting the current data from
    couchdb
    """
    requests = reqmgr_db_service.getRequestByNames(request_name)
    # generator object can't be subscribed: need to loop.
    # only one row should be returned
    for request in requests.values():
        current_state = request["RequestStatus"]
    if not check_allowed_transition(current_state, new_state):
        raise InvalidStateTransition(current_state, new_state)
    return


def create_json_template_spec(specArgs):
    template = {}
    for key, prop in specArgs.items():

        if key == "RequestorDN":
            # this will be automatically collected so skip it.
            continue

        if key == "CMSSWVersion":
            # get if from tag collector
            value = releases()
        elif key == "ScramArch":
            value = architectures()
        elif prop == "Dashboard":
            value = dashboardActivities()
        elif prop.get("optional", True):
            # if optional need to always have default value
            value = prop["default"]
        else:
            value = "REPLACE-%s" % key
        template[key] = value
    return template


def get_request_template_from_type(request_type, loc="WMSpec.StdSpecs"):
    pluginFactory = WMFactory("specArgs", loc)
    alteredClassName = "%sWorkloadFactory" % request_type
    spec = pluginFactory.loadObject(classname=request_type, alteredClassName=alteredClassName)
    specArgs = spec.getWorkloadArguments()

    result = create_json_template_spec(specArgs)
    return result


def validateOutputDatasets(outDsets, dbsUrl):
    """
    Validate output datasets after all the other arguments have been
    locally update during assignment
    """
    datatier = []
    for dataset in outDsets:
        tokens = dataset.split("/")
        procds = tokens[2]
        datatier.append(tokens[3])
        try:
            procdataset(procds)
        except AssertionError as ex:
            msg = "Bad output dataset name, check the processed dataset.\n %s" % str(ex)
            raise InvalidSpecParameterValue(msg)

    # Verify whether the output datatiers are available in DBS
    _validateDatatier(datatier, dbsUrl)


def _validateDatatier(datatier, dbsUrl):
    """
    _validateDatatier_

    Provided a list of datatiers extracted from the outputDatasets, checks
    whether they all exist in DBS already.
    """
    dbsTiers = DBSReader.listDatatiers(dbsUrl)
    badTiers = list(set(datatier) - set(dbsTiers))
    if badTiers:
        raise InvalidSpecParameterValue("Bad datatier(s): %s not available in DBS." % badTiers)
