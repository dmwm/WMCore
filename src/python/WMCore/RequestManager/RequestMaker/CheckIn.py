#!/usr/bin/env python
"""
_CheckIn_

CheckIn process for making a request

"""




import logging
import WMCore.RequestManager.RequestDB.Interface.Request.MakeRequest as MakeRequest
import WMCore.RequestManager.RequestDB.Interface.Admin.RequestManagement as RequestAdmin



def checkIn(request, workloadCache):
    """
    _CheckIn_

    Check in of a request manager

    Given a new request, check it in to the DB and add the
    appropriate IDs.
    """
    #  //
    # // First try and register the request in the DB
    #//
    workload = request['WorkflowSpec']
    request['RequestWorkflow'] = workloadCache.checkIn(workload)
    try:
        reqId = MakeRequest.createRequest(
        request['Requestor'],
        request['Group'],
        request['RequestName'],
        request['RequestType'],
        request['RequestWorkflow'],
        )
    except Exception, ex:
         msg = "Error creating new request:\n"
         msg += str(ex)
         raise RuntimeError, msg
    request['RequestID'] = reqId
    logging.info("Request %s created with request id %s" % (
        request['RequestName'], request['RequestID'])
                 )

    #  //
    # // add metadata about the request
    #//
    try:
        if request['InputDatasetTypes'] != {}:
            print "HAS TYPES?"
            for ds, dsType in request['InputDatasetTypes'].items():
                MakeRequest.associateInputDataset(
                    request['RequestName'], ds, dsType)
        elif isinstance(request['InputDatasets'], list):
            for ds in request['InputDatasets']:
                MakeRequest.associateInputDataset(request['RequestName'], ds)
        else:
            MakeRequest.associateInputDataset(request['RequestName'], request['InputDatasets'])
    except Exception, ex:
        msg = "Unable to Associate input datasets to request\n"
        msg += str(ex)
        msg += "\nUnable to check in new request"
        RequestAdmin.deleteRequest(request['RequestID'])
        workloadCache.remove(workload)
        raise RuntimeError, msg
    try:
        for ds in request['OutputDatasets']:
            MakeRequest.associateOutputDataset(request['RequestName'], ds)
    except Exception, ex:
        msg = "Unable to Associate output datasets to request\n"
        msg += str(ex)
        msg += "\nUnable to check in new request"
        RequestAdmin.deleteRequest(request['RequestID'])
        workloadCache.remove(workload)
        raise RuntimeError, msg


    try:
        for sw in request['SoftwareVersions']:
            MakeRequest.associateSoftware(
                request['RequestName'], sw)
    except Exception, ex:
        msg = "Unable to associate software for this request\n"
        msg += str(ex)
        msg += "\nUnable to check in new request"
        RequestAdmin.deleteRequest(request['RequestID'])
        workloadCache.remove(workload)
        raise RuntimeError, ex


        if request["RequestSizeEvents"] != None:
            MakeRequest.updateRequestSize(request['RequestName'],
                                          request["RequestSizeEvents"],
                                          request["RequestSizeFiles"]
                                          )


    return

