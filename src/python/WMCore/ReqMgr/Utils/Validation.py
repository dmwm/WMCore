"""
ReqMgr request handling.

"""
from __future__ import print_function
from future.utils import viewitems, viewvalues

from hashlib import md5

from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytesConditional
from WMCore.Lexicon import procdataset
from WMCore.REST.Auth import authz_match
from WMCore.ReqMgr.DataStructs.Request import initialize_request_args, initialize_clone
from WMCore.ReqMgr.DataStructs.RequestError import InvalidStateTransition, InvalidSpecParameterValue
from WMCore.ReqMgr.DataStructs.RequestStatus import check_allowed_transition, STATES_ALLOW_ONLY_STATE_TRANSITION
from WMCore.ReqMgr.Tools.cms import releases, architectures, dashboardActivities
from WMCore.Services.DBS.DBS3Reader import getDataTiers
from WMCore.WMFactory import WMFactory
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.WMWorkloadTools import loadSpecClassByType, setArgumentsWithDefault
from WMCore.Cache.GenericDataCache import GenericDataCache, MemoryCacheStruct

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

    # validate the status
    if "RequestStatus" in request_args:
        validate_state_transition(reqmgr_db_service, request_name, request_args["RequestStatus"])
        if request_args["RequestStatus"] in STATES_ALLOW_ONLY_STATE_TRANSITION:
            # if state change doesn't allow other transition nothing else to validate
            args_only_status = {}
            args_only_status["RequestStatus"] = request_args["RequestStatus"]
            args_only_status["cascade"] = request_args.get("cascade", False)
            return workload, args_only_status
        elif request_args["RequestStatus"] == 'assigned':
            workload.validateArgumentForAssignment(request_args)

    validate_request_priority(request_args)

    return workload, request_args


def validate_request_priority(reqArgs):
    """
    Validate the RequestPriority argument against its definition
    in StdBase
    :param reqArgs: dictionary of user request arguments
    :return: nothing, but raises an exception in case of an invalid value
    """
    if 'RequestPriority' in reqArgs:
        reqPrioDefin = StdBase.getWorkloadCreateArgs()['RequestPriority']
        if not isinstance(reqArgs['RequestPriority'], reqPrioDefin['type']):
            msg = "RequestPriority must be of integer type, not: {}".format(type(reqArgs['RequestPriority']))
            raise InvalidSpecParameterValue(msg)
        if reqPrioDefin['validate'](reqArgs['RequestPriority']) is False:
            raise InvalidSpecParameterValue("RequestPriority must be an integer between 0 and 999999")


def validate_request_create_args(request_args, config, reqmgr_db_service, *args, **kwargs):
    """
    *arg and **kwargs are only for the interface
    validate post request
    1. read data from body
    2. validate using spec validation
    3. convert data from body to arguments (spec instance, argument with default setting)
    TODO: raise right kind of error with clear message
    """
    if request_args["RequestType"] == "Resubmission":
        # do not set default values for Resubmission since it will be inherited from parent
        # both create & assign args are accepted for Resubmission creation
        workload, request_args = validate_resubmission_create_args(request_args, config, reqmgr_db_service)
    else:
        initialize_request_args(request_args, config)

        # load the correct class in order to validate the arguments
        specClass = loadSpecClassByType(request_args["RequestType"])
        # set default values for the request_args
        setArgumentsWithDefault(request_args, specClass.getWorkloadCreateArgs())
        spec = specClass()
        workload = spec.factoryWorkloadConstruction(request_args["RequestName"],
                                                    request_args)

    return workload, request_args


def validate_resubmission_create_args(request_args, config, reqmgr_db_service, *args, **kwargs):
    """
    Handle resubmission workflows, loading the spec arguments definition (and chain
    definition, if needed) and inheriting all arguments defined in the spec.
    User can also override arguments, since it uses the same mechanism as a clone.
    *arg and **kwargs are only for the interface
    """
    response = reqmgr_db_service.getRequestByNames(request_args["OriginalRequestName"])
    originalArgs = next(iter(viewvalues(response)))

    ### not a nice fix for #8245, but we cannot inherit the CollectionName attr
    originalArgs.pop("CollectionName", None)

    chainArgs = None
    if originalArgs["RequestType"] == 'Resubmission':
        # ACDC of ACDC, we can't validate this case
        # simply copy the whole original dictionary over and accept all args
        createArgs = originalArgs
        request_args["OriginalRequestType"] = originalArgs["OriginalRequestType"]
        request_args["ResubmissionCount"] = originalArgs.get("ResubmissionCount", 1) + 1
    else:
        # load arguments definition from the proper/original spec factory
        parentClass = loadSpecClassByType(originalArgs["RequestType"])
        createArgs = parentClass.getWorkloadAssignArgs()
        if originalArgs["RequestType"] in ('StepChain', 'TaskChain'):
            chainArgs = parentClass.getChainCreateArgs()
        createArgs.update(parentClass.getWorkloadCreateArgs())
        request_args["OriginalRequestType"] = originalArgs["RequestType"]

    cloned_args = initialize_clone(request_args, originalArgs, createArgs, chainArgs)
    initialize_request_args(cloned_args, config)

    specClass = loadSpecClassByType(request_args["RequestType"])
    spec = specClass()
    workload = spec.factoryWorkloadConstruction(cloned_args["RequestName"],
                                                cloned_args, request_args)

    return workload, cloned_args


def validate_clone_create_args(request_args, config, reqmgr_db_service, *args, **kwargs):
    """
    Handle clone workflows through the clone API, by loading the spec arguments
    definition (and chain definition, if needed) and inheriting all arguments defined
    in the spec.
    *arg and **kwargs are only for the interface
    """
    response = reqmgr_db_service.getRequestByNames(request_args.pop("OriginalRequestName"))
    originalArgs = next(iter(viewvalues(response)))

    chainArgs = None
    specClass = loadSpecClassByType(originalArgs["RequestType"])
    if originalArgs["RequestType"] == 'Resubmission':
        # cloning an ACDC, nothing that we can validate
        # simply copy the whole original dictionary over and accept all args
        createArgs = originalArgs
    else:
        # load arguments definition from the proper/original spec factory
        createArgs = specClass.getWorkloadCreateArgs()
        if originalArgs["RequestType"] in ('StepChain', 'TaskChain'):
            chainArgs = specClass.getChainCreateArgs()

    cloned_args = initialize_clone(request_args, originalArgs, createArgs, chainArgs)
    initialize_request_args(cloned_args, config)

    spec = specClass()
    workload = spec.factoryWorkloadConstruction(cloned_args["RequestName"], cloned_args)

    return workload, cloned_args


def validate_state_transition(reqmgr_db_service, request_name, new_state):
    """
    validate state transition by getting the current data from
    couchdb
    """
    requests = reqmgr_db_service.getRequestByNames(request_name)
    # generator object can't be subscribed: need to loop.
    # only one row should be returned
    for request in viewvalues(requests):
        current_state = request["RequestStatus"]
    if not check_allowed_transition(current_state, new_state):
        raise InvalidStateTransition(request_name, current_state, new_state)
    return


def create_json_template_spec(specArgs):
    template = {}
    for key, prop in viewitems(specArgs):

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
    specArgs = spec.getWorkloadCreateArgs()

    result = create_json_template_spec(specArgs)
    return result


def validateOutputDatasets(outDsets, dbsUrl):
    """
    Validate output datasets after all the other arguments have been
    locally update during assignment.
    """
    if len(outDsets) != len(set(outDsets)):
        msg = "Output dataset contains duplicates and it has to be fixed! %s" % outDsets
        raise InvalidSpecParameterValue(msg)

    datatier = []
    for dataset in outDsets:
        procds, tier = dataset.split("/")[2:]
        datatier.append(tier)
        try:
            procdataset(procds)
        except AssertionError as ex:
            msg = "Bad output dataset name, check the processed dataset name.\n %s" % str(ex)
            raise InvalidSpecParameterValue(msg)

    # TODO: this url conversion below can be removed in one year from now, thus March 2022
    dbsUrl = dbsUrl.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")

    # Verify whether the output datatiers are available in DBS
    _validateDatatier(datatier, dbsUrl)


def _validateDatatier(datatier, dbsUrl, expiration=3600):
    """
    _validateDatatier_

    Provided a list of datatiers extracted from the outputDatasets, checks
    whether they all exist in DBS.
    """
    cacheName = "dataTierList_" + md5(encodeUnicodeToBytesConditional(dbsUrl, condition=PY3)).hexdigest()
    if not GenericDataCache.cacheExists(cacheName):
        mc = MemoryCacheStruct(expiration, getDataTiers, kwargs={'dbsUrl': dbsUrl})
        GenericDataCache.registerCache(cacheName, mc)

    cacheData = GenericDataCache.getCacheData(cacheName)
    dbsTiers = cacheData.getData()
    badTiers = list(set(datatier) - set(dbsTiers))
    if badTiers:
        raise InvalidSpecParameterValue("Bad datatier(s): %s not available in DBS." % badTiers)


def isUserAllowed(authzCls, requestArgs):
    """
    Checks whether user is allowed to perform a given action,
    based on the request arguments provided and the user's
    roles and groups
    :param authzCls: an instance of the AuthzByStatus class
    :param requestArgs: dictionary with the arguments provided by the user
    :return: raise an HTTPError if the user is not allowed to
        perform action, otherwise return None
    """
    permission = authzCls.getRolesGroupsByStatus(requestArgs)
    authz_match(permission['role'], permission['group'])
