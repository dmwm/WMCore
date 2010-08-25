import urllib
from httplib import HTTPConnection
from WMCore.WebTools.Page import make_rfc_timestamp

def makeRequest(uri='/rest/', values=None, type='GET', accept="text/plain"):
    headers = {}
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": accept}
    data = None
    if values:
        data = urllib.urlencode(values)
    if type != 'POST' and data != None:
        uri = '%s?%s' % (uri, data)
    conn = HTTPConnection('localhost:8080')
    conn.connect()
    conn.request(type, uri, data, headers)
    response = conn.getresponse()
    
    data = response.read()
    conn.close()
    type = response.getheader('content-type').split(';')[0]
    return data, response.status, type, response

def methodTest(verb, url, input={}, accept='text/json', output={} , expireTime=300):
    
    data, code, type, response = makeRequest(url, input, verb, accept)
    
    keyMap = {'code': code, 'data': data, 'type': type, 'response': response}
    for key, value in output.items():
        assert keyMap[key] == value, \
            'Got a return %s != %s (got %s) (data %s)' % (key, value, code, data)
    
    expires = response.getheader('Expires')        
    assert expires == make_rfc_timestamp(expireTime), 'Expires header incorrect (%s)' % expires
    
    return data, expires