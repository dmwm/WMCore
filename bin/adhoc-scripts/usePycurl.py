import time
import types
import sys
from pprint import pformat
from urllib.parse import urlencode
from Utils.Utilities import decodeBytesToUnicode

from WMCore.Services.pycurl_manager import RequestHandler
from Utils.CertTools import getKeyCertFromEnv, getCAPathFromEnv

def main():
    #uri = "https://cmsweb.cern.ch/couchdb/_all_dbs"
    #uri = "https://cmsweb.cern.ch/couchdb/reqmgr_config_cache/6c075881a8454070ce3c1e8921cdb45e"
    uri = "https://cmsweb.cern.ch/couchdb/reqmgr_config_cache/6c075881a8454070ce3c1e8921cdb45e/configFile"

    ### deal with headers
    data = {}
    verb = "GET"
    incoming_headers = {}
    encoder = True
    contentType = None
    data, headers = encodeParams(data, verb, incoming_headers, encoder, contentType)

    headers["Accept-Encoding"] = "gzip,deflate,identity"
    ckey, cert = getKeyCertFromEnv()
    capath = getCAPathFromEnv()
    reqHandler = RequestHandler()
    timeStart = time.time()
    response, result = reqHandler.request(uri, data, headers, verb=verb, ckey=ckey, cert=cert, capath=capath)
    timeEnd = time.time()
    print(f"Uri: {uri}")
    print(f"Request headers: {headers}")
    print(f"Time: {timeEnd - timeStart}")
    print(f"Response headers: {pformat(response.header)}")

    ### deal with the response object
    decoder = True
    result = decodeResult(result, decoder)
    #print(f"Response: {result}")


def encodeParams(data, verb, incomingHeaders, encoder, contentType):
    headers = {"Content-type": contentType if contentType else "application/json",
               "User-Agent": "WMCore/usePycurl",
               "Accept": "application/json"}

    incomingHeaders["Accept-Encoding"] = "gzip,identity"
    headers.update(incomingHeaders)

    encoded_data = ''
    if verb != 'GET' and data:
        if isinstance(encoder, (types.MethodType, types.FunctionType)):
            encoded_data = encoder(data)
        elif encoder is False:
            encoded_data = data
        else:
            encoded_data = self.encode(data)
        headers["Content-Length"] = len(encoded_data)
    elif verb != 'GET':
        headers["Content-Length"] = 0
    elif verb == 'GET' and data:
        # encode the data as a get string
        encoded_data = urlencode(data, doseq=True)
    return encoded_data, headers

def decodeResult(result, decoder):
    if isinstance(decoder, (types.MethodType, types.FunctionType)):
        result = decoder(result)
    elif decoder is not False:
        result = decodeBytesToUnicode(result)
    return result

if __name__ == '__main__':
    sys.exit(main())