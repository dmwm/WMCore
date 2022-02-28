#!/usr/bin/env python
"""
Basic interface to HTTPS requests using ssl object managers, for python >= 2.7.9.
See usage example in:
src/python/WMCore/WMSpec/Steps/Executors/DQMUpload.py
"""
from __future__ import print_function, division

import logging
import ssl
try:
    # python2
    import urllib2
    import httplib
    HTTPSHandler = urllib2.HTTPSHandler
    HTTPSConnection = httplib.HTTPSConnection
except:
    # python3
    import urllib.request
    import http.client
    HTTPSHandler = urllib.request.HTTPSHandler
    HTTPSConnection = http.client.HTTPSConnection


class HTTPSAuthHandler(HTTPSHandler):
    """
    HTTPS authentication class to provide a ssl context with the certificates.
    """
    def __init__(self, key=None, cert=None, capath='/etc/grid-security/certificates/', level=0):
        self.logger = logging.getLogger(__name__)
        if cert:
            # then create a default ssl context manager to carry the credentials.
            # It also loads the default CA certificates
            self.ctx = ssl.create_default_context()
            self.ctx.load_cert_chain(cert, keyfile=key)
            self.ctx.load_verify_locations(None, capath)

            self.logger.info("Found %d default trusted CA certificates.", len(self.ctx.get_ca_certs()))
            ### DEBUG start ###
            #for ca in self.ctx.get_ca_certs():
            #    if 'CERN' in str(ca['subject']):
            #        print("  %s" % str(ca['subject']))
            ### DEBUG end ###
            self.logger.info("SSL context manager created with the following settings:")
            self.logger.info("  check_hostname : %s", self.ctx.check_hostname)  # default to True
            self.logger.info("  options : %s", self.ctx.options)  # default to 2197947391
            self.logger.info("  protocol : %s", self.ctx.protocol)  # default to 2 (PROTOCOL_SSLv23)
            self.logger.info("  verify_flags : %s", self.ctx.verify_flags)  # default to 0 (VERIFY_DEFAULT)
            self.logger.info("  verify_mode : %s", self.ctx.verify_mode)  # default to 2 (CERT_REQUIRED)
            HTTPSHandler.__init__(self, debuglevel=level, context=self.ctx)
        else:
            self.logger.info("Certificate not provided for HTTPSHandler")
            HTTPSHandler.__init__(self, debuglevel=level)

    def get_connection(self, host, **kwargs):
        if self.ctx:
            return HTTPSConnection(host, context=self.ctx, **kwargs)
        return HTTPSConnection(host)

    def https_open(self, req):
        """
        Overwrite the default https_open.
        """
        self.logger.debug("%s method to %s", req.get_method(), req.get_full_url())
        self.logger.debug("  with the following headers: %s", req.headers)
        return self.do_open(self.get_connection, req)
