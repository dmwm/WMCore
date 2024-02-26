"""
This module contains a few basic WMCore-related utilitarian
MSPileup functions
"""

from Utils.CertTools import ckey, cert

from WMCore.Services.pycurl_manager import RequestHandler


def getPileupDocs(mspileupUrl, queryDict=None, method='GET'):
    """
    Fetch documents from MSPileup according to the query passed in using POST.

    :param mspileupUrl: string with the MSPileup url
    :param queryDict: dictionary with the MongoDB query to run
    :param method: string, defines which HTTP method to use
    :return: returns a list with all the pileup objects, or raises
        an exception in case of failure
    """
    queryDict = queryDict or {}
    mgr = RequestHandler()
    headers = {'Content-Type': 'application/json'}
    data = mgr.getdata(mspileupUrl, queryDict, headers, verb=method,
                       ckey=ckey(), cert=cert(), encode=True, decode=True)
    if data and data.get("result", []):
        if "error" in data["result"][0]:
            msg = f"Failed to retrieve MSPileup documents with query: {queryDict}"
            msg += f" and error message: {data}"
            raise RuntimeError(msg)
    return data["result"]
