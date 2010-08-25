#
# It uses urllib2 instead of pure httplib to avoid processing
# directly handling HTTP(S), doing properly redirections, handling cookies
# and implementing browser-like behavior in a RFC compliant.
#
# Using urllib2 openers allows us to plugin an openid handler transparentely.
# It could also be used in conjunction to other handlers such as proxies or
# even application-specific ones; it could also be used in the default urllib2
# opener so that any urllib2.urlopen() request will be aware of openid.
#
# Unfortunately, the HTTPS handler from urllib2 does not allow to pass
# cert and key as arguments to HTTPSConnection. To use certificate
# authentication, you should properly set default
# HTTPSConnection class attributes (key and cert) before requesting
# to open an openid protected service url.
# ** TO DO: include cert and key arguments to HTTPS handler **
# The HTTPS handler may not exist if python wasn't compiled with
# ssl support. In this case, it is not possible to set default key
# and cert.

import urllib, urllib2, cookielib
import socket
import httplib

class HTTPOpenIDProcessor(urllib2.BaseHandler):
    def __init__(self, oidserver='http://localhost:8400', username='', password=''):
        self.oidserver = oidserver
        self.username = username
        self.password = password

    # This must be the last response processor in the chain (after http(s)
    # and the cookie response processors)
    def openid_response(self, req, resp):
        if resp.geturl().startswith(self.oidserver+'/openidserver') and \
               resp.getcode() == 200:
            # Means that the oidserver responded with the login form
            login_data = urllib.urlencode({'identifier' : self.username,
                                           'password' : self.password,
                                           'fail_to' : '/login',
                                           'submit' : 'login'})
            # ** To fix ** : Should check the answer returned by the loginsubmit
            # and redirect to the fail_to accordingly
            r=self.parent.open(self.oidserver+'/loginsubmit', login_data, req.timeout)
            if r.geturl().startswith(self.oidserver+'/login'):
                return resp # user/pass failed. Return what it initially returned 

            # Continues from the point it was...
            resp = self.parent.open(resp.geturl(), req.data, req.timeout)
        return resp

    http_response = openid_response
    https_response = openid_response

def get_opener(oidserver='https://cmsweb.cern.ch/security/',user='',passw='',cookies=None):
    """
    Creates an url opener aware of openid.
    """
    # Creates a new cookie db in memory
    if not cookies:
        cookies = cookielib.CookieJar()

    # the order of handlers is important
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookies),
                                  HTTPOpenIDProcessor(oidserver, user, passw))
    # https should be detected automatically, or we could accept config here and pass it to the HTTPSHandler
    return opener

def set_def_opener(opener):
    # The provided opener will be the default used by urllib2.urlopen().
    urllib2.install_opener(opener)

# if hasattr(httplib, 'HTTPS'):
#     class HTTPSHandler(AbstractHTTPHandler):

#         def https_open(self, req):
#             # ** this must be reimplemented to get use key and cert
#             return self.do_open(httplib.HTTPSConnection, req)

#         https_request = AbstractHTTPHandler.do_request_


if __name__ == "__main__":
    cli_simon=get_opener(oidserver='http://localhost:8400',user='simon',passw='password')
    r=cli_simon.open('http://localhost:8212/securedocumentation/')
    print r.read()
    print r.getcode()

    cli_diego=get_opener(oidserver='http://localhost:8400',user='diego',passw='password123')
    r=cli_diego.open('http://localhost:8212/securedocumentation/')
    print r.read()
    print r.getcode()
