"""
_WMWorkloadTools_

Define some generic tools used by the StdSpecs and WMWorkload
to validate arguments that modify a WMWorkload and/or WMTask.

Created on Jun 13, 2013

@author: dballest
"""
import json
import re
import inspect

from Utils.Utilities import makeList
from WMCore.DataStructs.LumiList import LumiList
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDEx_VALID_SUBSCRIPTION_PRIORITIES


def makeLumiList(lumiDict):
    try:
        if isinstance(lumiDict, basestring):
            lumiDict = json.loads(lumiDict)
        ll = LumiList(compactList=lumiDict)
        return ll.getCompactList()
    except:
        raise WMSpecFactoryException("Could not parse LumiList, %s: %s" % (type(lumiDict), lumiDict))


def parsePileupConfig(mcPileup, dataPileup):
    """
    _parsePileupConfig_

    If the pileup config is defined as MCPileup and DataPileup
    then make sure we get the usual dictionary as
    PileupConfig : {'mc': ['/mc_pd/procds/tier'], 'data': ['/data_pd/procds/tier']}
    """
    pileUpConfig = {}
    if mcPileup is not None:
        pileUpConfig['mc'] = [mcPileup]
    if dataPileup is not None:
        pileUpConfig['data'] = [dataPileup]
    return pileUpConfig


def _validateArgument(argument, value, argumentDefinition):
    """
    Validate a single argument against its definition in the spec
    """
    validNull = argumentDefinition["null"]
    if not validNull and value is None:
        raise WMSpecFactoryException("Argument '%s' cannot be None" % argument)
    elif value is None:
        return value

    try:
        value = argumentDefinition["type"](value)
    except Exception:
        msg = "Argument '%s' with value %r, has an incorrect data type: " % (argument, value)
        msg += "%s. It must be %s" % (type(value), argumentDefinition["type"])
        raise WMSpecFactoryException(msg)

    _validateArgFunction(argument, value, argumentDefinition["validate"])
    return value


def _validateArgumentDict(argument, argValue, argumentDefinition):
    """
    Validate only the basic structure of dict arguments, we anyways
    don't have the definition of the internal arguments.
    """
    # make sure we're not going to cast a dict to string and let that unnoticed
    if isinstance(argumentDefinition["type"], type(dict)) and not isinstance(argValue, dict):
        msg = "Argument '%s' with value %r, has an incorrect data type: " % (argument, argValue)
        msg += "%s. It must be %s" % (type(argValue), argumentDefinition["type"])
        raise WMSpecFactoryException(msg)

    # still an exception, make sure it has the correct format
    if argument == "LumiList":
        argValue = argumentDefinition["type"](argValue)

    _validateArgFunction(argument, argValue, argumentDefinition["validate"])
    return argValue


def _validateArgFunction(argument, value, valFunction):
    """
    Perform the validation function as in the argument definition
    """
    if valFunction:
        try:
            if not valFunction(value):
                msg = "Argument '%s' with value %r, doesn't pass the validate function." % (argument, value)
                msg += "\nIt's definition is:\n%s" % inspect.getsource(valFunction)
                raise WMSpecFactoryException(msg)
        except WMSpecFactoryException:
            # just re-raise it to keep the error message clear
            raise
        except Exception as ex:
            # Some validation functions (e.g. Lexicon) will raise errors instead of returning False
            raise WMSpecFactoryException(str(ex))
    return


def _validateArgumentOptions(arguments, argumentDefinition, optionKey=None):
    """
    Check whether create or assign mandatory parameters were properly
    set in the request schema.
    """
    for arg, argDef in argumentDefinition.iteritems():
        optional = argDef.get(optionKey, True)
        if not optional and arg not in arguments:
            msg = "Argument '%s' is mandatory! Its definition is:\n%s" % (arg, inspect.getsource(argDef))
            raise WMSpecFactoryException(msg)
        # specific case when user GUI returns empty string for optional arguments
        elif arg not in arguments:
            continue
        elif isinstance(arguments[arg], dict):
            arguments[arg] = _validateArgumentDict(arg, arguments[arg], argDef)
        else:
            arguments[arg] = _validateArgument(arg, arguments[arg], argDef)
    return


def validateInputDatasSetAndParentFlag(arguments):
    """
    Check if the InputDataset value provided corresponds to an actual dataset in DBS.
    If parent flag is provided, then check whether the input dataset has a parent.
    the InputDataset existence in DBS and its parent, if needed.
    """
    inputdataset = _getChainKey(arguments, "InputDataset")
    mcpileup = _getChainKey(arguments, "MCPileup")
    datapileup = _getChainKey(arguments, "DataPileup")
    includeParents = _getChainKey(arguments, "IncludeParents")
    dbsURL = arguments.get("DbsUrl")

    if includeParents and not inputdataset:
        msg = "IncludeParents flag is True but InputDataset value has not been provided"
        raise WMSpecFactoryException(msg)

    if dbsURL and inputdataset or mcpileup or datapileup:
        # import DBS3Reader here, since Runtime code import this module and worker
        # node doesn't have dbs3 client
        from WMCore.Services.DBS.DBS3Reader import DBS3Reader
        from WMCore.Services.DBS.DBSErrors import DBSReaderError
        dbsInst = DBS3Reader(dbsURL)

        try:
            _datasetExists(dbsInst, inputdataset)
            _datasetExists(dbsInst, mcpileup)
            _datasetExists(dbsInst, datapileup)
        except DBSReaderError as ex:
            # we need to Wrap the exception to WMSpecFactoryException to be caught in reqmgr validation
            raise WMSpecFactoryException(str(ex))

        if includeParents:
            try:
                result = dbsInst.listDatasetParents(inputdataset)
                if len(result) == 0:
                    msg = "IncludeParents flag is True but the input dataset %s has no parents" % inputdataset
                    raise DBSReaderError(msg)
            except DBSReaderError as ex:
                raise WMSpecFactoryException(str(ex))

    return


def _datasetExists(dbsInst, inputData):
    """
    __datasetExists_

    Check if dataset exists in DBS. Exception is raised in case it does not exist.
    """
    if inputData is None:
        return
    dbsInst.checkDatasetPath(inputData)
    return


def _getChainKey(arguments, keyName):
    """
    Given a request arguments dictionary and a key name, properly returns its
    value regardless of the request type.
    """
    if "TaskChain" in arguments:
        value = arguments['Task1'].get(keyName)
    elif "StepChain" in arguments:
        value = arguments['Step1'].get(keyName)
    else:
        value = arguments.get(keyName)
    return value


def validatePhEDExSubscription(arguments):
    """
    _validatePhEDExSubscription_

    Validate all the PhEDEx arguments provided during request
    creation and assignment.
    """
    for site in arguments.get("AutoApproveSubscriptionSites", []):
        if site.endswith('_MSS'):
            raise WMSpecFactoryException("Auto-approval to MSS endpoint is not allowed: %s" % site)
    if arguments.get("SubscriptionPriority", "Low").lower() not in PhEDEx_VALID_SUBSCRIPTION_PRIORITIES:
        raise WMSpecFactoryException("Invalid subscription priority: %s" % arguments["SubscriptionPriority"])
    if arguments.get("CustodialSubType", "Replica") not in ["Move", "Replica"]:
        raise WMSpecFactoryException("Invalid custodial subscription type: %s" % arguments["CustodialSubType"])
    if arguments.get("NonCustodialSubType", "Replica") not in ["Move", "Replica"]:
        raise WMSpecFactoryException("Invalid non custodial subscription type: %s" % arguments["NonCustodialSubType"])

    if arguments.get("CustodialSubType") == "Move":
        _validateMoveSubscription("CustodialSubType", arguments.get('CustodialSites', []))
    if arguments.get("NonCustodialSubType") == "Move":
        _validateMoveSubscription("NonCustodialSubType", arguments.get('NonCustodialSites', []))

    return


def _validateMoveSubscription(subType, sites):
    """
    Move subscriptions are only allowed to T0 or T1s, see #7760
    """
    invalidSites = [site for site in sites if re.match("^T[2-3]", site)]
    if invalidSites:
        msg = "Move subscription (%s) not allowed to T2/T3 sites: %s" % (subType, invalidSites)
        raise WMSpecFactoryException(msg)


def validateSiteLists(arguments):
    whiteList = arguments.get("SiteWhitelist", [])
    blackList = arguments.get("SiteBlacklist", [])
    whiteList = makeList(whiteList)
    blackList = makeList(blackList)
    res = (set(whiteList) & set(blackList))
    if len(res):
        msg = "Validation failed: The same site cannot be white and blacklisted: %s" % list(res)
        raise WMSpecFactoryException(msg)
    # store the properly formatted values (list instead of string)
    arguments["SiteWhitelist"] = whiteList
    arguments["SiteBlacklist"] = blackList
    return


def validateArgumentsCreate(arguments, argumentDefinition, checkInputDset=True):
    """
    _validateArguments_

    Validate a set of arguments - for spec creation - against their
    definition in StdBase.getWorkloadCreateArgs.

    When validating Step/Task dictionary, checkInputDset should be usually
    false since the input dataset validation already happened at top level.

    It returns an error message if the validation went wrong,
    otherwise returns None.
    """
    validateUnknownArgs(arguments, argumentDefinition)
    _validateArgumentOptions(arguments, argumentDefinition, "optional")
    if checkInputDset:
        validateInputDatasSetAndParentFlag(arguments)
    return


def validateArgumentsUpdate(arguments, argumentDefinition):
    """
    _validateArgumentsUpdate_

    Validate a set of arguments - for spec assignment/update - against
    their definition in StdBase.getWorkloadAssignArgs.

    It returns an error message if the validation went wrong,
    otherwise returns None.
    """
    validateUnknownArgs(arguments, argumentDefinition)
    _validateArgumentOptions(arguments, argumentDefinition, "assign_optional")
    validatePhEDExSubscription(arguments)
    validateSiteLists(arguments)

    return

def validateUnknownArgs(arguments, argumentDefinition):
    """
    Make sure user is sending only arguments that are known by
    StdBase.getWorkloadCreateArgs, otherwise fail spec creation.

    It returns an error message if the validation went wrong,
    otherwise returns None.
    """
    unknownArgs = set(arguments) - set(argumentDefinition.keys())
    if unknownArgs:
        # now onto the exceptions...
        if arguments.get("RequestType") == "ReReco":
            unknownArgs = unknownArgs - set([x for x in unknownArgs if x.startswith("Skim")])
        elif arguments.get("RequestType") == "StepChain":
            unknownArgs = unknownArgs - set([x for x in unknownArgs if x.startswith("Step")])
        elif arguments.get("RequestType") == "TaskChain":
            unknownArgs = unknownArgs - set([x for x in unknownArgs if x.startswith("Task")])
        elif arguments.get("RequestType") == "Resubmission":
            # oh well, then we have to skip all possible obscure arguments
            unknownArgs = unknownArgs - set([x for x in unknownArgs if x.startswith("Skim")])
            unknownArgs = unknownArgs - set([x for x in unknownArgs if x.startswith("Step")])
            unknownArgs = unknownArgs - set([x for x in unknownArgs if x.startswith("Task")])

        if unknownArgs:
            msg = "There are unknown/unsupported arguments in your request spec: %s" % list(unknownArgs)
            raise WMSpecFactoryException(msg)
    return


def setArgumentsWithDefault(arguments, argumentDefinition):
    """
    Set arguments not provided by the user with a default spec value
    """
    for argument in argumentDefinition:
        if argument not in arguments:
            arguments[argument] = argumentDefinition[argument]["default"]

    # set the Campaign default value to the same as AcquisitionEra if Campaign is not specified
    if "Campaign" in argumentDefinition and not arguments.get("Campaign"):
        if "AcquisitionEra" in arguments and isinstance(arguments["AcquisitionEra"], basestring):
            arguments["Campaign"] = arguments["AcquisitionEra"]

    return

def setAssignArgumentsWithDefault(arguments, argumentDefinition):
    """
    Set arguments not provided by the user with a default assign spec value,
    unless the default value is None (read don't set default).
    """
    for argument in argumentDefinition:
        if argument not in arguments and argumentDefinition[argument]["default"] is not None:
            arguments[argument] = argumentDefinition[argument]["default"]

    return

def loadSpecClassByType(specType):
    factoryName = "%sWorkloadFactory" % specType
    mod = __import__("WMCore.WMSpec.StdSpecs.%s" % specType,
                     globals(), locals(), [factoryName])
    specClass = getattr(mod, factoryName)

    return specClass


def loadSpecByType(specType):
    specClass = loadSpecClassByType(specType)
    return specClass()


def checkDBSURL(url):
    # import DBS3Reader here, since Runtime code import this module and worker node doesn't have dbs3 client
    from WMCore.Services.DBS.DBS3Reader import DBS3Reader
    return DBS3Reader(url).checkDBSServer()
