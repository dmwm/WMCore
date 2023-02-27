#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-
# pylint: disable=R0913,W0702,R0914,R0912,R0201
"""
File: pycurl_manager.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: a basic wrapper around pycurl library.
The RequestHandler class provides basic APIs to get data
from a single resource or submit mutliple requests to
underlying data-services.

Examples:
# CERN SSO: http://linux.web.cern.ch/linux/docs/cernssocookie.shtml
# use RequestHandler with CERN SSO enabled site
mgr = RequestHandler()
url = "https://cms-gwmsmon.cern.ch/prodview/json/site_summary"
params = {}
tfile = tempfile.NamedTemporaryFile()
cern_sso_cookie(url, tfile.name, cert, ckey)
cookie = {url: tfile.name}
header, data = mgr.request(url3, params, cookie=cookie)
if header.status != 200:
    print "ERROR"

# fetch multiple urls at onces from various urls
tfile = tempfile.NamedTemporaryFile()
ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
url1 = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader/help"
url2 = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader/datatiers"
url3 = "https://cms-gwmsmon.cern.ch/prodview/json/site_summary"
cern_sso_cookie(url3, tfile.name, cert, ckey)
cookie = {url3: tfile.name}
urls = [url1, url2, url3]
data = getdata(urls, ckey, cert, cookie=cookie)
for row in data:
    print(row)
"""
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()

from builtins import str, range, object
from past.builtins import basestring
from future.utils import viewitems


# system modules
import copy
import json
import gzip
import logging
import os
import re
import subprocess
import pycurl
from io import BytesIO
import http.client
from urllib.parse import urlencode

from Utils.Utilities import encodeUnicodeToBytes, decodeBytesToUnicode
from Utils.PortForward import portForward, PortForward
from Utils.TokenManager import TokenManager


def getException(url, params, headers, header, data):
    """
    Return HTTP exception for a given set of parameters:
    :param url: string
    :param params: dict
    :param headers: dict
    :param header: ResponseHeader
    :param data: HTTP body
    """
    msg = 'url=%s, code=%s, reason=%s, headers=%s, result=%s' \
          % (url, header.status, header.reason, header.header, data)
    exc = http.client.HTTPException(msg)
    setattr(exc, 'req_data', params)
    setattr(exc, 'req_headers', headers)
    setattr(exc, 'url', url)
    setattr(exc, 'result', data)
    setattr(exc, 'status', header.status)
    setattr(exc, 'reason', header.reason)
    setattr(exc, 'headers', header.header)
    return exc

def decompress(body, headers):
    """
    Helper function to decompress given body if HTTP headers contains gzip encoding
    :param body: bytes
    :param headers: dict
    :return: decode body
    """
    encoding = ""
    for header, value in headers.items():
        if header.lower() == 'content-encoding' and 'gzip' in value.lower():
            encoding = 'gzip'
            break
    if encoding != 'gzip':
        return body

    try:
        return gzip.decompress(body)
    except Exception as exc:
        logger = logging.getLogger()
        msg = "While processing decompress function with headers: %s, " % headers
        msg += "we were unable to decompress gzip content. Details: %s. " % str(exc)
        msg += "Considering response body as uncompressed."
        logger.exception(msg)
        return body

class ResponseHeader(object):
    """ResponseHeader parses HTTP response header"""

    def __init__(self, response):
        super(ResponseHeader, self).__init__()
        self.header = {}
        self.reason = ''
        self.fromcache = False
        self.parse(response)

    def parse(self, response):
        """Parse response header and assign class member data"""
        startRegex = r"^HTTP/(\d|\d.\d) \d{3}" # to match "HTTP/1.1 200" and "HTTP/2 200"
        continueRegex = r"^HTTP/(\d|\d.\d) 100"  # Continue: client should continue its request

        response = decodeBytesToUnicode(response)

        for row in response.split('\r'):
            row = row.replace('\n', '')
            if not row:
                continue
            if re.search(startRegex, row):
                if re.search(continueRegex, row):
                    continue
                # split HTTP header row on empty space
                # for HTTP/proto STATUS REASON
                arr = row.split(' ')
                self.status = int(arr[1])
                self.reason = ' '.join(arr[2:])
                continue
            try:
                key, val = row.split(':', 1)
                self.header[key.strip()] = val.strip()
            except:
                pass

    def getReason(self):
        """
        Return the HTTP request reason
        """
        return self.reason

    def getHeader(self):
        """
        Return the header dictionary object
        """
        return self.header

    def getHeaderKey(self, keyName):
        """
        Provided a key name, return it from the HTTP header.
        Note that - by design - header keys are meant to be
        case insensitive
        :param keyName: a header key name to be looked up
        :return: the value for that header key, or None if not found
        """
        for keyHea, valHea in self.header.items():
            if keyHea.lower() == keyName.lower():
                return valHea


class RequestHandler(object):
    """
    RequestHandler provides APIs to fetch single/multiple
    URL requests based on pycurl library
    """

    def __init__(self, config=None, logger=None):
        super(RequestHandler, self).__init__()
        if not config:
            config = {}
        defaultOpts = pycurl_options()
        self.nosignal = config.get('nosignal', defaultOpts['NOSIGNAL'])
        self.timeout = config.get('timeout', defaultOpts['TIMEOUT'])
        self.connecttimeout = config.get('connecttimeout', defaultOpts['CONNECTTIMEOUT'])
        self.followlocation = config.get('followlocation', defaultOpts['FOLLOWLOCATION'])
        self.maxredirs = config.get('maxredirs', defaultOpts['MAXREDIRS'])
        self.logger = logger if logger else logging.getLogger()
        self.tokenLocation = config.get('iam_token_file', '')
        if self.tokenLocation:
            self.tmgr = TokenManager(self.tokenLocation)
        else:
            self.tmgr = None

    def encode_params(self, params, verb, doseq, encode):
        """ Encode request parameters for usage with the 4 verbs.
            Assume params is already encoded if it is a string and
            uses a different encoding depending on the HTTP verb
            (either json.dumps or urllib.urlencode)
        """
        if not encode:
            return params
        # data is already encoded, just return it
        if isinstance(params, basestring):
            return params

        # data is not encoded, we need to do that
        if verb in ['GET', 'HEAD']:
            if params:
                encoded_data = urlencode(params, doseq=doseq)
            else:
                return ''
        else:
            if params:
                encoded_data = json.dumps(params)
            else:
                return {}

        return encoded_data

    def set_opts(self, curl, url, params, headers,
                 ckey=None, cert=None, capath=None, verbose=None,
                 verb='GET', doseq=True, encode=False, cainfo=None, cookie=None):
        """Set options for given curl object, params should be a dictionary"""
        if not (isinstance(params, (dict, basestring)) or params is None):
            raise TypeError("pycurl parameters should be passed as dictionary or an (encoded) string")
        #  ensure the original headers object remains unchanged
        headers = headers or {}  # if it's None, then make it a dict
        thisHeaders = copy.deepcopy(headers)
        curl.setopt(pycurl.NOSIGNAL, self.nosignal)
        curl.setopt(pycurl.TIMEOUT, self.timeout)
        curl.setopt(pycurl.CONNECTTIMEOUT, self.connecttimeout)
        curl.setopt(pycurl.FOLLOWLOCATION, self.followlocation)
        curl.setopt(pycurl.MAXREDIRS, self.maxredirs)

        if cookie and url in cookie:
            curl.setopt(pycurl.COOKIEFILE, cookie[url])
            curl.setopt(pycurl.COOKIEJAR, cookie[url])

        encoded_data = self.encode_params(params, verb, doseq, encode)

        if verb == 'GET':
            if encoded_data:
                url = url + '?' + encoded_data
        elif verb == 'HEAD':
            if encoded_data:
                url = url + '?' + encoded_data
            curl.setopt(pycurl.CUSTOMREQUEST, verb)
            curl.setopt(pycurl.HEADER, 1)
            curl.setopt(pycurl.NOBODY, True)
        elif verb == 'POST':
            curl.setopt(pycurl.POST, 1)
            if encoded_data:
                curl.setopt(pycurl.POSTFIELDS, encoded_data)
        elif verb == 'DELETE' or verb == 'PUT':
            curl.setopt(pycurl.CUSTOMREQUEST, verb)
            curl.setopt(pycurl.HTTPHEADER, ['Transfer-Encoding: chunked'])
            if encoded_data:
                curl.setopt(pycurl.POSTFIELDS, encoded_data)
        else:
            raise Exception('Unsupported HTTP method "%s"' % verb)

        if self.tmgr:
            token = self.tmgr.getToken()
            if token:
                thisHeaders['Authorization'] = 'Bearer {}'.format(token)

        if verb in ('POST', 'PUT'):
            # only these methods (and PATCH) require this header
            thisHeaders["Content-Length"] = str(len(encoded_data))

        # we must pass url as a bytes data-type, otherwise pycurl will fail with error
        # TypeError: invalid arguments to setopt
        # see https://curl.haxx.se/mail/curlpython-2007-07/0001.html
        curl.setopt(pycurl.URL, encodeUnicodeToBytes(url))
        # In order to enable service intercommunication with compressed HTTP body,
        # we need to enable this header here, in case it has not been provided by upstream.
        thisHeaders.setdefault("Accept-Encoding", "gzip")
        curl.setopt(pycurl.HTTPHEADER, [encodeUnicodeToBytes("%s: %s" % (k, v)) for k, v in viewitems(thisHeaders)])

        bbuf = BytesIO()
        hbuf = BytesIO()
        curl.setopt(pycurl.WRITEFUNCTION, bbuf.write)
        curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        if capath:
            curl.setopt(pycurl.CAPATH, capath)
            curl.setopt(pycurl.SSL_VERIFYPEER, True)
            if cainfo:
                curl.setopt(pycurl.CAINFO, cainfo)
        else:
            curl.setopt(pycurl.SSL_VERIFYPEER, False)
        if ckey:
            curl.setopt(pycurl.SSLKEY, ckey)
        if cert:
            curl.setopt(pycurl.SSLCERT, cert)
        if verbose:
            curl.setopt(pycurl.VERBOSE, True)
            curl.setopt(pycurl.DEBUGFUNCTION, self.debug)
        return bbuf, hbuf

    def debug(self, debug_type, debug_msg):
        """Debug callback implementation"""
        print("debug(%d): %s" % (debug_type, debug_msg))

    def parse_body(self, data, decode=False):
        """
        Parse body part of URL request (by default use json).
        This method can be overwritten.
        """
        if decode:
            try:
                res = json.loads(data)
                return res
            except ValueError as exc:
                msg = 'Unable to load JSON data, %s, data type=%s, pass as is' \
                      % (str(exc), type(data))
                logging.debug(msg)
                return data
        else:
            return data

    def parse_header(self, header):
        """
        Parse response header.
        This method can be overwritten.
        """
        return ResponseHeader(header)

    @portForward(8443)
    def request(self, url, params, headers=None, verb='GET',
                verbose=0, ckey=None, cert=None, capath=None,
                doseq=True, encode=False, decode=False, cainfo=None, cookie=None):
        """Fetch data for given set of parameters"""
        curl = pycurl.Curl()
        bbuf, hbuf = self.set_opts(curl, url, params, headers, ckey, cert, capath,
                                   verbose, verb, doseq, encode, cainfo, cookie)
        curl.perform()
        if verbose:
            print(verb, url, params, headers)
        header = self.parse_header(hbuf.getvalue())
        data = bbuf.getvalue()
        data = decompress(data, header.header)
        if header.status < 300:
            if verb == 'HEAD':
                data = ''
            else:
                data = self.parse_body(data, decode)
        else:
            exc = getException(url, params, headers, header, data)
            bbuf.flush()
            hbuf.flush()
            raise exc

        bbuf.flush()
        hbuf.flush()
        return header, data

    def getdata(self, url, params, headers=None, verb='GET',
                verbose=0, ckey=None, cert=None, doseq=True,
                encode=False, decode=False, cookie=None):
        """Fetch data for given set of parameters"""
        _, data = self.request(url=url, params=params, headers=headers, verb=verb,
                               verbose=verbose, ckey=ckey, cert=cert, doseq=doseq,
                               encode=encode, decode=decode, cookie=cookie)
        return data

    def getheader(self, url, params, headers=None, verb='GET',
                  verbose=0, ckey=None, cert=None, doseq=True):
        """Fetch HTTP header"""
        header, _ = self.request(url, params, headers, verb,
                                 verbose, ckey, cert, doseq=doseq)
        return header

    @portForward(8443)
    def multirequest(self, url, parray, headers=None, verb='GET',
                     ckey=None, cert=None, verbose=None, cookie=None,
                     encode=False, decode=False):
        """Fetch data for given set of parameters"""
        multi = pycurl.CurlMulti()
        for params in parray:
            curl = pycurl.Curl()
            bbuf, hbuf = \
                self.set_opts(curl, url, params, headers, ckey=ckey, cert=cert,
                              verbose=verbose, cookie=cookie, encode=encode)
            multi.add_handle(curl)
            while True:
                ret, num_handles = multi.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break
            while num_handles:
                ret = multi.select(1.0)
                if ret == -1:
                    continue
                while True:
                    ret, num_handles = multi.perform()
                    if ret != pycurl.E_CALL_MULTI_PERFORM:
                        break
            dummyNumq, response, dummyErr = multi.info_read()
            for _respItem in response:
                header = self.parse_header(hbuf.getvalue())
                data = bbuf.getvalue()
                data = decompress(data, header.header)
                data = decodeBytesToUnicode(data)
                if header.status < 300:
                    if verb == 'HEAD':
                        data = ''
                    else:
                        data = self.parse_body(data, decode)
                else:
                    exc = getException(url, params, headers, header, data)
                    bbuf.flush()
                    hbuf.flush()
                    raise exc
                if isinstance(data, dict):
                    data.update(params)
                    yield data
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            item.update(params)
                            yield item
                        else:
                            err = 'Unsupported data format: data=%s, type=%s' \
                                  % (item, type(item))
                            raise Exception(err)
                bbuf.flush()
                hbuf.flush()


HTTP_PAT = re.compile( \
    "(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]")


def validate_url(url):
    "Validate URL"
    if HTTP_PAT.match(url):
        return True
    return False


def pycurl_options():
    "Default set of options for pycurl"
    opts = {
        'FOLLOWLOCATION': 1,
        'CONNECTTIMEOUT': 270,
        'MAXREDIRS': 5,
        'NOSIGNAL': 1,
        'TIMEOUT': 270,
        'SSL_VERIFYPEER': False,
        'VERBOSE': 0
    }
    return opts


def cern_sso_cookie(url, fname, cert, ckey):
    "Obtain cern SSO cookie and store it in given file name"
    cmd = 'cern-get-sso-cookie -cert %s -key %s -r -u %s -o %s' \
          % (cert, ckey, url, fname)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=os.environ)
    proc.wait()


def getdata(urls, ckey, cert, headers=None, options=None, num_conn=50, cookie=None):
    """
    Get data for given list of urls, using provided number of connections
    and user credentials
    """

    if not options:
        options = pycurl_options()

    portForwarder = PortForward(8443)

    # Make a queue with urls
    queue = [portForwarder(u) for u in urls if validate_url(u)]

    # Check args
    num_urls = len(queue)
    num_conn = min(num_conn, num_urls)

    # Pre-allocate a list of curl objects
    mcurl = pycurl.CurlMulti()
    mcurl.handles = []
    for _ in range(num_conn):
        curl = pycurl.Curl()
        curl.fp = None
        for key, val in viewitems(options):
            curl.setopt(getattr(pycurl, key), val)
        curl.setopt(pycurl.SSLKEY, ckey)
        curl.setopt(pycurl.SSLCERT, cert)
        mcurl.handles.append(curl)
        if headers:
            curl.setopt(pycurl.HTTPHEADER, \
                        ["%s: %s" % (k, v) for k, v in viewitems(headers)])

    # Main loop
    freelist = mcurl.handles[:]
    num_processed = 0
    while num_processed < num_urls:
        # If there is an url to process and a free curl object,
        # add to multi-stack
        while queue and freelist:
            url = queue.pop(0)
            curl = freelist.pop()
            curl.setopt(pycurl.URL, url.encode('ascii', 'ignore'))
            if cookie and url in cookie:
                curl.setopt(pycurl.COOKIEFILE, cookie[url])
                curl.setopt(pycurl.COOKIEJAR, cookie[url])
            bbuf = BytesIO()
            hbuf = BytesIO()
            curl.setopt(pycurl.WRITEFUNCTION, bbuf.write)
            curl.setopt(pycurl.HEADERFUNCTION, hbuf.write)
            mcurl.add_handle(curl)
            # store some info
            curl.hbuf = hbuf
            curl.bbuf = bbuf
            curl.url = url
        # Run the internal curl state machine for the multi stack
        while True:
            ret, _ = mcurl.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated, and add them to the
        # freelist
        while True:
            num_q, ok_list, err_list = mcurl.info_read()
            for curl in ok_list:
                hdrs = decodeBytesToUnicode(curl.hbuf.getvalue())
                data = decompress(decodeBytesToUnicode(curl.bbuf.getvalue()), ResponseHeader(hdrs).getHeader())
                url = curl.url
                curl.bbuf.flush()
                curl.bbuf.close()
                curl.hbuf.close()
                curl.hbuf = None
                curl.bbuf = None
                mcurl.remove_handle(curl)
                freelist.append(curl)
                yield {'url': url, 'data': data, 'headers': hdrs}
            for curl, errno, errmsg in err_list:
                hdrs = curl.hbuf.getvalue()
                data = curl.bbuf.getvalue()
                url = curl.url
                curl.bbuf.flush()
                curl.bbuf.close()
                curl.hbuf.close()
                curl.hbuf = None
                curl.bbuf = None
                mcurl.remove_handle(curl)
                freelist.append(curl)
                yield {'url': url, 'data': None, 'headers': hdrs, \
                       'error': errmsg, 'code': errno}
            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break
        # Currently no more I/O is pending, could do something in the meantime
        # (display a progress bar, etc.).
        # We just call select() to sleep until some more data is available.
        mcurl.select(1.0)

    cleanup(mcurl)


def cleanup(mcurl):
    "Clean-up MultiCurl handles"
    for curl in mcurl.handles:
        if curl.hbuf is not None:
            curl.hbuf.close()
            curl.hbuf = None
        if curl.bbuf is not None:
            curl.bbuf.close()
            curl.bbuf = None
        curl.close()
    mcurl.close()
