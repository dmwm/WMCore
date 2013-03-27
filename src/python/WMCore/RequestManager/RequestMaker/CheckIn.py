#!/usr/bin/env python
"""
_CheckIn_

CheckIn process for making a request

"""

import sys
import logging
import traceback
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Request.MakeRequest as MakeRequest
import WMCore.RequestManager.RequestDB.Interface.Request.Campaign as Campaign
import WMCore.RequestManager.RequestDB.Interface.Admin.RequestManagement as RequestAdmin
import WMCore.RequestManager.RequestDB.Interface.Admin.SoftwareManagement as SoftwareManagement

from WMCore.WMException         import WMException


class RequestCheckInError(WMException):
    """
    Reporting errors when a problem with check-in operations occurs
    """


def _raiseCheckInError(request, ex, msg):
    """
    Private function called only from this module.
    Always from the except exception block - the traceback
    is put in the local log file.

    """
    requestName = request['RequestName']
    msg +='\n' + str(ex)
    msg += "\nUnable to check in new request %s" % requestName

    reqId = GetRequest.requestID(requestName)
    # make absolutely sure you're deleting the right one
    oldReqId = request['RequestID']
    if reqId:
        # make absolutely sure you're deleting the right one
        oldReqId = request['RequestID']
        if oldReqId != reqId:
            raise RequestCheckInError("Bad state deleting request %s/%s.  Please contact a ReqMgr administrator" % (oldReqId/ reqId))
        else:
            RequestAdmin.deleteRequest(requestName)
    # get information about the last exception
    trace = traceback.format_exception(*sys.exc_info())
    traceString = ''.join(trace)
    logging.error("%s\n%s" % (msg, traceString))
    raise RequestCheckInError(msg)


def checkIn(request, requestType = 'None'):
    """
    _CheckIn_

    Check in of a request manager

    Given a new request, check it in to the DB and add the
    appropriate IDs.
    """
    #  //
    # // First try and register the request in the DB
    #//
    requestName = request['RequestName']
    
    # test if the software versions are registered first
    versions  = SoftwareManagement.listSoftware()
    scramArch = request.get('ScramArch')
    if requestType.lower() in ['resubmission']:
        # Do nothing, as we have no valid software version yet
        pass
    else:
        if not scramArch in versions.keys():
            m = ("Cannot find scramArch %s in ReqMgr (the one(s) available: %s)" %
                 (scramArch, versions))
            raise RequestCheckInError(m)
        for version in request.get('SoftwareVersions', []):
            if not version in versions[scramArch]:
                raise RequestCheckInError("Cannot find software version %s in ReqMgr for "
                                          "scramArch %s. Supported versions: %s" %
                                          (version, scramArch, versions[scramArch]))

    try:
        reqId = MakeRequest.createRequest(
        request['Requestor'],
        request['Group'],
        requestName,
        request['RequestType'],
        request['RequestWorkflow'],
        request.get('PrepID', None),
        request.get('RequestPriority', None)
    )
    except Exception, ex:
        msg = "Error creating new request:\n"
        msg += str(ex)
        raise RequestCheckInError( msg )
    #FIXME LAST_INSERT_ID doesn't work on oracle
    reqId = GetRequest.requestID(requestName)
    request['RequestID'] = reqId
    logging.info("Request %s created with request id %s" % (
        requestName, request['RequestID'])
                 )

    #  //
    # // add metadata about the request
    #//
    try:
        if request['InputDatasetTypes'] != {}:
            for ds, dsType in request['InputDatasetTypes'].items():
                MakeRequest.associateInputDataset(
                    requestName, ds, dsType)
        elif isinstance(request['InputDatasets'], list):
            for ds in request['InputDatasets']:
                MakeRequest.associateInputDataset(requestName, ds)
        else:
            MakeRequest.associateInputDataset(requestName, request['InputDatasets'])
    except Exception, ex:
        _raiseCheckInError(request, ex, "Unable to Associate input datasets to request")
    try:
        for ds in request['OutputDatasets']:
            # request['OutputDatasets'] may contain a list of lists (each sublist for a task)
            # which is actually not understood why but seems to be correct (Steve)
            # dirty
            if isinstance(ds, list):
                for dss in ds:
                    MakeRequest.associateOutputDataset(requestName, dss)
            else:
                MakeRequest.associateOutputDataset(requestName, ds)
    except Exception, ex:
        _raiseCheckInError(request, ex, "Unable to Associate output datasets to request")

    try:
        for sw in request['SoftwareVersions']:
            MakeRequest.associateSoftware(requestName, sw)
    except Exception, ex:
        _raiseCheckInError(request, ex, "Unable to associate software for this request")

    if request["RequestNumEvents"] != None:
        MakeRequest.updateRequestSize(requestName, request["RequestNumEvents"],
                                      request.get("RequestSizeFiles", 0),
                                      request.get("SizePerEvent", 0))
        
    campaign = request.get("Campaign", "")
    if campaign != "" and campaign != None:
        Campaign.associateCampaign(campaign, reqId)
        
    logging.info("Request '%s' built with request id '%s"'' % (requestName,
                request['RequestID']))