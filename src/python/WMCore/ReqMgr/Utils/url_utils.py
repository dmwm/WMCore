#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : url_utils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""
from __future__ import print_function

# system modules
import os
import sys
import urllib
import urllib2
import httplib
import json

def get_key_cert():
    """
    Get user key/certificate
    """
    key  = None
    cert = None
    globus_key  = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
    globus_cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
    if  os.path.isfile(globus_key):
        key  = globus_key
    if  os.path.isfile(globus_cert):
        cert  = globus_cert

    # First presendence to HOST Certificate, RARE
    if  'X509_HOST_CERT' in os.environ:
        cert = os.environ['X509_HOST_CERT']
        key  = os.environ['X509_HOST_KEY']

    # Second preference to User Proxy, very common
    elif 'X509_USER_PROXY' in os.environ:
        cert = os.environ['X509_USER_PROXY']
        key  = cert

    # Third preference to User Cert/Proxy combinition
    elif 'X509_USER_CERT' in os.environ:
        cert = os.environ['X509_USER_CERT']
        key  = os.environ['X509_USER_KEY']

    # Worst case, look for cert at default location /tmp/x509up_u$uid
    elif not key or not cert:
        uid  = os.getuid()
        cert = '/tmp/x509up_u'+str(uid)
        key  = cert

    if  not os.path.exists(cert):
        raise Exception("Certificate PEM file %s not found" % key)
    if  not os.path.exists(key):
        raise Exception("Key PEM file %s not found" % key)

    return key, cert

def disable_urllib2Proxy():
    """
    Setup once and forever urllib2 proxy, see
    http://kember.net/articles/obscure-python-urllib2-proxy-gotcha
    """
    proxy_support = urllib2.ProxyHandler({})
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level > 1:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def getdata(url, params, headers=None, post=None, verbose=False, jsondecoder=True):
    """
    Invoke URL call and retrieve data from data-service based
    on provided URL and set of parameters. Use post=True to
    invoke POST request.
    """
    encoded_data = urllib.urlencode(params)
    if  not post:
        if  encoded_data:
            url = url + '?' + encoded_data
    if  not headers:
        headers = {}
    if  verbose:
        print('+++ getdata, url=%s, headers=%s' % (url, headers))
    obj=sys.version_info
    if  obj[0] == 2 and obj[1] == 7 and obj[2] >= 9:
        # disable SSL verification, since it is default in python 2.7.9
        # and many CMS services do not verify SSL cert.
        # https://www.python.org/dev/peps/pep-0476/
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context
    req = urllib2.Request(url)
    for key, val in headers.iteritems():
        req.add_header(key, val)
    if  verbose > 1:
        handler = urllib2.HTTPHandler(debuglevel=1)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    ckey, cert = get_key_cert()
    handler = HTTPSClientAuthHandler(ckey, cert, verbose)
    if  verbose:
        print("handler", handler, handler.__dict__)
    opener  = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    try:
        if  post:
            data = urllib2.urlopen(req, encoded_data)
        else:
            data = urllib2.urlopen(req)
        info = data.info()
        code = data.getcode()
        if  verbose > 1:
            print("+++ response code:", code)
            print("+++ response info\n", info)
        if  jsondecoder:
            data = json.load(data)
        else:
            data = data.read()
    except urllib2.HTTPError as httperror:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s' \
                    % (url, params, headers)
        data = {'error': 'Unable to contact %s' % url , 'reason': msg}
        try:
            data.update({'httperror':extract_http_error(httperror.read())})
        except Exception as exp:
            data.update({'httperror': None})
        data = json.dumps(data)
    except Exception as exp:
        msg  = 'HTTPError, url=%s, args=%s, headers=%s, error=%s' \
                    % (url, params, headers, str(exp))
        data = {'error': 'Unable to contact %s' % url, 'reason': msg}
        data = json.dumps(data)
    return data
