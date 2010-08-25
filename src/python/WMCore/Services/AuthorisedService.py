#!/usr/bin/env python
"""
_AuthorisedService_

An AuthorisedService is the same as a Service but sends a cert/key with the url
opener to access secured resources.
"""

__revision__ = "$Id: AuthorisedService.py,v 1.6 2009/07/08 14:45:55 sryu Exp $"
__version__ = "$Revision: 1.6 $"

import datetime, os, urllib, time

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.Services.Service import Service

def getKeyCert():
    """
       _getKeyCert_
       
       Gets the User Proxy if it exists otherwise throws an exception
       This code is bollowed from DBSAPI/dbsHttpService.py with Anzar's permission
    """

    # First presendence to HOST Certificate, This is how it set in Tier0
    if os.environ.has_key('X509_HOST_CERT'):
        cert = os.environ['X509_HOST_CERT']
        key = os.environ['X509_HOST_KEY']
        
    # Second preference to User Proxy, very common
    elif os.environ.has_key('X509_USER_PROXY'):
        cert = os.environ['X509_USER_PROXY']
        key = cert

    # Third preference to User Cert/Proxy combinition
    elif os.environ.has_key('X509_USER_CERT'):
        cert = os.environ['X509_USER_CERT']
        key = os.environ['X509_USER_KEY']
    
    #TODO: only in linux, unix case, add other os case
    # Worst case, look for proxy at default location /tmp/x509up_u$uid
    else :
        uid = os.getuid()
        cert = '/tmp/x509up_u'+str(uid)
        key = cert

    #Set but not found
    if not os.path.exists(cert) or not os.path.exists(key):
        msg = "Certificate is not found"
        raise Exception, msg
    
    # All looks OK, still doesn't gurantee proxy's validity etc.
    return key, cert

class AuthorisedService(Service):
    def __init__(self, dict={}):
        Service.__init__(self, dict)
        try:
            if not dict.has_key('key') or not dict.has_key('cert'):
                dict['key'], dict['cert'] = getKeyCert()
                
            self.cert = dict['cert']
            self.key = dict['key']
                
        except:
            self.logger.exception('Service requires a host certificate and key')
            raise WMException('Service requires a host certificate and key', 
                              "WMCORE-11") 
    
    def _getURLOpener(self):
        """
        method getting url opener, it is used by getData method
        sub class can override this to have different URL opener
        i.e. - if it needs authentication
        """
        return urllib.URLopener(cert_file=self.cert, key_file=self.key)
