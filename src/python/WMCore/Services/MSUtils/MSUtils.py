"""
This module contains a few basic WMCore-related utilitarian functions
"""

from Utils.CertTools import ckey, cert

from WMCore.Services.pycurl_manager import RequestHandler


def makeHttpRequest(srvUrl, payload=None, method='GET', headers=None, encode=True, decode=True, payloadSizeToDump=50):
    """
    Perform HTTP call to provided service URL. This can be any of supported methods like
    GET, POST, PUT, DELETE. It will return HTTP response from upstream MicroService
    and parse it accordingly (the WMCore MS service returns results as {'result': {'data':...}}
    HTTP response.

    :param srvUrl: string with the service url
    :param payload: payload query or document, in case of GET HTTP request it is query dictionary,
        in case of POST/PUT HTTP requests it is JSON payload to the service
    :param method: string, defines which HTTP method to use
    :param headers: HTTP headers (presented as dictionarY)
    :param encode: boolean to reflect if data should be encoded, see pycurl_manager.py
    :param decode: boolean to reflect if data should be decoded, see pycurl_manager.py
    :param payloadSizeToDump: amount of characters to dump in a log from payload (to make log entries readable)
    :return: returns a list with all objects, or raises
        an exception in case of failure
    """
    payload = payload or {}
    mgr = RequestHandler()
    headers = headers or {'Content-Type': 'application/json'}
    data = mgr.getdata(srvUrl, payload, headers, verb=method,
                       ckey=ckey(), cert=cert(), encode=encode, decode=decode)
    if data and data.get("result", []):
        if "error" in data["result"][0]:
            # strip off part of payload to make readable log message
            sdata = f"{payload}"
            if len(sdata) > payloadSizeToDump:
                sdata = sdata[:payloadSizeToDump] + "..."
            msg = f"Failed to contact {srvUrl} via {method} request with {sdata}"
            msg += f" and error message: {data}"
            raise RuntimeError(msg)
    return data["result"]

def getPileupDocs(mspileupUrl, queryDict=None, method='GET'):
    """
    Fetch documents from MSPileup according to the query passed in using POST.

    :param mspileupUrl: string with the MSPileup url
    :param queryDict: dictionary with the MongoDB query to run
    :param method: string, defines which HTTP method to use
    :return: returns a list with all the pileup objects, or raises
        an exception in case of failure
    """
    return makeHttpRequest(mspileupUrl, payload=queryDict, method=method)
