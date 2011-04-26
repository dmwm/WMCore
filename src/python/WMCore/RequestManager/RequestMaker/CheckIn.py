#!/usr/bin/env python
"""
_CheckIn_

CheckIn process for making a request

"""




import logging
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest
import WMCore.RequestManager.RequestDB.Interface.Request.MakeRequest as MakeRequest
import WMCore.RequestManager.RequestDB.Interface.Request.Campaign as Campaign
import WMCore.RequestManager.RequestDB.Interface.Admin.RequestManagement as RequestAdmin


def raiseCheckInError(request, ex, msg):
    msg +='\n' + str(ex)
    msg += "\nUnable to check in new request"
    RequestAdmin.deleteRequest(request['RequestID'])
    raise RuntimeError, msg


def checkIn(request):
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
    try:
        reqId = MakeRequest.createRequest(
        request['Requestor'],
        request['Group'],
        requestName,
        request['RequestType'],
        request['RequestWorkflow'],
    )
    except Exception, ex:
        msg = "Error creating new request:\n"
        msg += str(ex)
        raise RuntimeError, msg
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
        raiseCheckInError(request, ex, "Unable to Associate input datasets to request")
    try:
        for ds in request['OutputDatasets']:
            MakeRequest.associateOutputDataset(requestName, ds)
    except Exception, ex:
        raiseCheckInError(request, ex, "Unable to Associate output datasets to request")

    try:
        for sw in request['SoftwareVersions']:
            MakeRequest.associateSoftware(requestName, sw)
    except Exception, ex:
        raiseCheckInError(request, ex, "Unable to associate software for this request")

    if request["RequestSizeEvents"] != None:
        MakeRequest.updateRequestSize(requestName, request["RequestSizeEvents"],
                                      request.get("RequestSizeFiles", 0)
                                      )
    campaign = request.get("Campaign", "")
    if campaign != "" and campaign != None:
        Campaign.associateCampaign(campaign, reqId)

    return

