""" Provides a command-line client for accessing openid-protected services

It uses urllib2 instead of pure httplib to avoid handling
directly HTTP(S), doing properly redirections, handling cookies
and implementing browser-like behavior in a RFC compliant manner.

Using urllib2 openers allowed to plugin an openid handler transparentely.
It could also be used in conjunction to other handlers such as proxies or
even application-specific ones; it could also be used in the default urllib2
opener so that any urllib2.urlopen() request would be aware of openid.

It also provides authentication through user certificates (the default
HTTPS handler that comes with urllib2 does not support it).

If the openid server supports user certificate authentication, this will be
the first attempt. Then it will try username/password auth. If none worked, it 
returns the login page from the oid server.

Once the client authenticated, it will use cookies to keep a session. It is
possible to share the same cookie db with many url openers (much like a user
browser does with multiple windows/tabs). The default behavior is not to share
the cookie db, so that each opener can login with a different username.

Examples:
# Get an url opener as the diego user, using certificate authentication
diego=get_opener(oidserver='http://localhost:8400'
                 key_file='/home/dmwm/diegocert.pem',
                 cert_file='/home/dmwm/diegokey.pem')
r=diego.open('http://localhost:8212/securedocumentation/')
print r.read()

# Get an url opener as the simon user, using username authentication
simon=get_opener(oidserver='http://localhost:8400',
                 user='simon', passw='password')
r=simon.open('http://localhost:8212/securedocumentation/')
print r.read()

# Seting an opener to be the default one for urlopen()
urllib2.install_opener(simon)
urllib2.urlopen('http://localhost:8212/securedocumentation/')

You can also use it to open SSL urls that require user certificates, but that
not necessarily support openid. For instance:

cli=get_opener(key_file='/home/dmwm/mycert.pem',
               cert_file='/home/dmwm/mykey.pem')
cli.open('https://cmsweb.cern.ch/phedex/')

For more information about open() (for instance, if you want to
POST data), please refer to the urllib2 docs:
http://docs.python.org/library/urllib2.html

"""

import urllib2
from cookielib import CookieJar
from urllib import urlencode
from socket import _GLOBAL_DEFAULT_TIMEOUT
from httplib import HTTPSConnection

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
            login_data = urlencode({'identifier' : self.username,
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

# This must be a subclass of HTTPSHandler so that the
# urllib2.build_opener() will not include HTTPSHandler but
# this class instead
class HTTPSCertAwareHandler(urllib2.HTTPSHandler):
    def __init__(self, key_file=None, cert_file=None):
        urllib2.HTTPSHandler.__init__(self)
        self.kf = key_file
        self.cf = cert_file

    def https_open(self, req):
        return self.do_open(lambda h, timeout=_GLOBAL_DEFAULT_TIMEOUT:
                               HTTPSConnection(h, timeout=timeout,
                                               key_file=self.kf,
                                               cert_file=self.cf),
                            req)

def get_opener(oidserver='https://cmsweb.cern.ch/security/',
               key_file=None, cert_file=None,
               cookies=None,
               user='', passw=''):
    """
    Creates an url opener aware of openid.
    """
    # Creates a new cookie db in memory
    if not cookies:
        cookies = CookieJar()

    # the order of handlers in this chain matters
    opener = urllib2.build_opener(HTTPSCertAwareHandler(key_file, cert_file),   #order: 1st
                                  urllib2.HTTPCookieProcessor(cookies),         #       2nd
                                  HTTPOpenIDProcessor(oidserver, user, passw))  #       last
    return opener

def set_def_opener(opener):
    # The provided opener will be the default used by urllib2.urlopen().
    urllib2.install_opener(opener)

# Here it goes some examples on how to use this client interface
if __name__ == "__main__":
    cli_simon=get_opener(oidserver='http://localhost:8400',user='simon',passw='password')
    r=cli_simon.open('http://localhost:8212/securedocumentation/')
    print r.read()
    print r.getcode()

    cli_diego=get_opener(oidserver='http://localhost:8400',user='diego',passw='password123')
    r=cli_diego.open('http://localhost:8212/securedocumentation/')
    print r.read()
    print r.getcode()

    cli_diegocert=get_opener(oidserver='http://localhost:8400',
                             key_file='/home/dmwm/diegonewkeyunlocked.pem',
                             cert_file='/home/dmwm/diegonewcert.pem')
    print cli_diegocert.open('http://localhost:8212/securedocumentation/').read()

    # It can also be used to open other non-openid but certificate-protected urls 
    print cli_diegocert.open('https://cmsweb.cern.ch/phedex/').read()
