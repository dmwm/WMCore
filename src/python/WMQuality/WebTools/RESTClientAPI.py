import urllib
from urlparse import urlparse
from httplib import HTTPConnection
from WMCore.WebTools.Page import make_rfc_timestamp
from WMCore.Wrappers import JsonWrapper
    
def makeRequest(url, values=None, verb='GET', accept="text/plain", 
                contentType = None):
    headers = {}
    contentType = contentType or "application/x-www-form-urlencoded"
    headers = {"content-type": contentType,
               "Accept": accept}
    data = None
    if verb == 'GET' and values:
        data = urllib.urlencode(values, doseq=True)
    elif verb != 'GET' and values:
        # needs to test other encoding type
        if contentType == "application/x-www-form-urlencoded":
            data = urllib.urlencode(values)
        else:
            # for other encoding scheme values assumed to be encoded already
            data = values
    parser = urlparse(url)
    uri = parser.path
    if parser.query:
        uri += "?" + parser.query
        
    if verb != 'POST' and data != None:
        uri = '%s?%s' % (uri, data)
        
    # need to specify Content-length for POST method
    # TODO: this function needs refactoring - too verb-related branching
    if verb == "POST":
        if data:
            print "POST method, data: '%s' len: '%s'" % (data, len(data))
            headers.update({"content-length": len(data)})
        else:
            print "POST method, data: '%s'" % data
            headers.update({"content-length" : 0})
        
    conn = HTTPConnection(parser.netloc)
    conn.connect()
    conn.request(verb, uri, data, headers)
    response = conn.getresponse()
    
    data = response.read()
    conn.close()
    cType = response.getheader('content-type').split(';')[0]
    return data, response.status, cType, response

def methodTest(verb, url, input={}, accept='text/json', contentType = None, output={} , expireTime=300):
    
    data, code, type, response = makeRequest(url, input, verb, accept, contentType)
    
    keyMap = {'code': code, 'data': data, 'type': type, 'response': response}
    for key, value in output.items():
        assert keyMap[key] == value, \
            'Got a return %s != %s (got %s) (data %s)' % (key, value, keyMap[key], data)
    
    expires = response.getheader('Expires')
    timeStamp = make_rfc_timestamp(expireTime)        
    assert expires == timeStamp,\
             'Expires header incorrect (%s) != (%s)' % (expires % timeStamp)
    
    return data, expires