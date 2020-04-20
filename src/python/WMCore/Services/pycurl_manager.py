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
url1 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/help"
url2 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers"
url3 = "https://cms-gwmsmon.cern.ch/prodview/json/site_summary"
cern_sso_cookie(url3, tfile.name, cert, ckey)
cookie = {url3: tfile.name}
urls = [url1, url2, url3]
data = getdata(urls, ckey, cert, cookie=cookie)
for row in data:
    print(row)
"""
from __future__ import print_function

import cStringIO as StringIO
import httplib
import json
import logging
import os
import re
import subprocess
import sys

try:
    from urllib import urlencode
except ImportError:
    # PY3
    from urllib.parse import urlencode

# python3
if sys.version.startswith('3.'):
    import io

# 3d-party libraries
import pycurl


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
        startRegex = r"^HTTP/\d.\d \d{3}"
        continueRegex = r"^HTTP/\d.\d 100"  # Continue: client should continue its request
        replaceRegex = r"^HTTP/\d.\d"

        for row in response.split('\r'):
            row = row.replace('\n', '')
            if not row:
                continue
            if re.search(startRegex, row):
                if re.search(continueRegex, row):
                    continue
                res = re.sub(replaceRegex, "", row).strip()
                status, reason = res.split(' ', 1)
                self.status = int(status)
                self.reason = reason
                continue
            try:
                key, val = row.split(':', 1)
                self.header[key.strip()] = val.strip()
            except:
                pass


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
        curl.setopt(pycurl.NOSIGNAL, self.nosignal)
        curl.setopt(pycurl.TIMEOUT, self.timeout)
        curl.setopt(pycurl.CONNECTTIMEOUT, self.connecttimeout)
        curl.setopt(pycurl.FOLLOWLOCATION, self.followlocation)
        curl.setopt(pycurl.MAXREDIRS, self.maxredirs)

        # also accepts encoding/compression algorithms
        if headers and headers.get("Accept-Encoding"):
            if isinstance(headers["Accept-Encoding"], basestring):
                curl.setopt(pycurl.ENCODING, headers["Accept-Encoding"])
            else:
                logging.warning("Wrong data type for header 'Accept-Encoding': %s",
                                type(headers["Accept-Encoding"]))

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

        if verb in ('POST', 'PUT'):
            # only these methods (and PATCH) require this header
            headers["Content-Length"] = str(len(encoded_data))

        # we must pass url as a string data-type, otherwise pycurl will fail with error
        # TypeError: invalid arguments to setopt
        # see https://curl.haxx.se/mail/curlpython-2007-07/0001.html
        curl.setopt(pycurl.URL, str(url))
        if headers:
            curl.setopt(pycurl.HTTPHEADER, \
                        ["%s: %s" % (k, v) for k, v in headers.items()])
        bbuf = StringIO.StringIO()
        hbuf = StringIO.StringIO()
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
        if header.status < 300:
            if verb == 'HEAD':
                data = ''
            else:
                data = self.parse_body(bbuf.getvalue(), decode)
        else:
            data = bbuf.getvalue()
            msg = 'url=%s, code=%s, reason=%s, headers=%s' \
                  % (url, header.status, header.reason, header.header)
            exc = httplib.HTTPException(msg)
            setattr(exc, 'req_data', params)
            setattr(exc, 'req_headers', headers)
            setattr(exc, 'url', url)
            setattr(exc, 'result', data)
            setattr(exc, 'status', header.status)
            setattr(exc, 'reason', header.reason)
            setattr(exc, 'headers', header.header)
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
                                 verbose, ckey, cert, doseq)
        return header

    def multirequest(self, url, parray, headers=None,
                     ckey=None, cert=None, verbose=None, cookie=None):
        """Fetch data for given set of parameters"""
        multi = pycurl.CurlMulti()
        for params in parray:
            curl = pycurl.Curl()
            bbuf, hbuf = \
                self.set_opts(curl, url, params, headers, ckey=ckey, cert=cert,
                              verbose=verbose, cookie=cookie)
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
            for dummyCobj in response:
                data = json.loads(bbuf.getvalue())
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


def getdata(urls, ckey, cert, headers=None, options=None, num_conn=100, cookie=None):
    """
    Get data for given list of urls, using provided number of connections
    and user credentials
    """

    if not options:
        options = pycurl_options()

    # Make a queue with urls
    queue = [u for u in urls if validate_url(u)]

    # Check args
    num_urls = len(queue)
    num_conn = min(num_conn, num_urls)

    # Pre-allocate a list of curl objects
    mcurl = pycurl.CurlMulti()
    mcurl.handles = []
    for _ in range(num_conn):
        curl = pycurl.Curl()
        curl.fp = None
        for key, val in options.items():
            curl.setopt(getattr(pycurl, key), val)
        curl.setopt(pycurl.SSLKEY, ckey)
        curl.setopt(pycurl.SSLCERT, cert)
        mcurl.handles.append(curl)
        if headers:
            curl.setopt(pycurl.HTTPHEADER, \
                        ["%s: %s" % (k, v) for k, v in headers.items()])

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
            if sys.version.startswith('3.'):
                bbuf = io.BytesIO()
                hbuf = io.BytesIO()
            else:
                bbuf = StringIO.StringIO()
                hbuf = StringIO.StringIO()
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
                if sys.version.startswith('3.'):
                    hdrs = curl.hbuf.getvalue().decode('utf-8')
                    data = curl.bbuf.getvalue().decode('utf-8')
                else:
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
