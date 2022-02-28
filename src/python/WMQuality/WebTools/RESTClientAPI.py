from future import standard_library
standard_library.install_aliases()

from future.utils import viewitems

import hashlib
import hmac
import urllib.parse

from http.client import HTTPConnection

from Utils.Utilities import decodeBytesToUnicodeConditional, encodeUnicodeToBytesConditional
from Utils.PythonVersion import PY3

from WMCore.WebTools.Page import make_rfc_timestamp


def makeRequest(url, values=None, verb='GET', accept="text/plain",
                contentType=None, secure=False, secureParam={}):
    """
    :rtype: (bytes (both py2 and py3), int, str (native), 
            (instance httplib.HTTPResponse in py2, 'http.client.HTTPResponse' in py3))
    """
    headers = {}
    contentType = contentType or "application/x-www-form-urlencoded"
    headers = {"content-type": contentType,
               "Accept": accept,
               "cms-auth-status": "NONE"}
    if secure:
        headers.update({"cms-auth-status": "OK",
                        "cms-authn-dn": "/DC=ch/OU=Organic Units/OU=Users/CN=Fake User",
                        "cms-authn-name": "Fake User",
                        "cms-authz-%s" % secureParam['role']:
                            "group:%s site:%s" % (secureParam['group'],
                                                  secureParam['site'])})
        headers["cms-authn-hmac"] = _generateHash(secureParam["key"], headers)

    data = None
    if verb == 'GET' and values:
        data = urllib.parse.urlencode(values, doseq=True)
    elif verb != 'GET' and values:
        # needs to test other encoding type
        if contentType == "application/x-www-form-urlencoded":
            data = urllib.parse.urlencode(values)
        else:
            # for other encoding scheme values assumed to be encoded already
            data = values
    parser = urllib.parse.urlparse(url)
    uri = parser.path
    if parser.query:
        uri += "?" + parser.query

    if verb == 'GET' and data != None:
        uri = '%s?%s' % (uri, data)

    # need to specify Content-length for POST method
    # TODO: this function needs refactoring - too verb-related branching
    if verb != 'GET':
        if data:
            headers.update({"content-length": len(data)})
        else:
            headers.update({"content-length": 0})

    conn = HTTPConnection(parser.netloc)
    conn.connect()
    conn.request(verb, uri, data, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    cType = response.getheader('content-type').split(';')[0]
    # data returned could be something a json like: b'"foo"', so we need to properly load it
    #if '/json' in accept:
    #    data = json.loads(data)
    return data, response.status, cType, response


def methodTest(verb, url, request_input={}, accept='text/json', contentType=None,
               output={}, expireTime=0, secure=False, secureParam={}):
    data, code, content_type, response = makeRequest(url, request_input, verb,
                                                     accept, contentType,
                                                     secure, secureParam)

    data = decodeBytesToUnicodeConditional(data, condition=PY3)

    keyMap = {'code': code, 'data': data, 'type': content_type, 'response': response}
    for key, value in viewitems(output):
        msg = 'Got a return %s != %s (got %s, type %s) (expected %s, type %s)' \
              % (keyMap[key], value, keyMap[key], type(keyMap[key]), value, type(value))
        assert keyMap[key] == value, msg

    expires = response.getheader('Expires')
    if expireTime != 0:
        timeStamp = make_rfc_timestamp(expireTime)
        assert expires == timeStamp, \
            'Expires header incorrect (%s) != (%s)' % (expires, timeStamp)

    return data, expires


def _generateHash(keyfile, headers):
    prefix = suffix = ""
    hkeys = sorted(headers.keys())
    for hk in hkeys:
        hk = hk.lower()
        if hk[0:9] in ["cms-authn", "cms-authz"]:
            prefix += "h%xv%x" % (len(hk), len(headers[hk]))
            suffix += "%s%s" % (hk, headers[hk])

    return hmac.new(keyfile, prefix + "#" + suffix, hashlib.sha1).hexdigest()
