#!/usr/bin/env python
"""
_PortForward_

A decorator for swapping ports in an url
"""
from __future__ import print_function, division
from future import standard_library
standard_library.install_aliases()

from builtins import str

import logging
from urllib.parse import urlparse, ParseResult


def portForward(port):
    """
    Decorator wrapper function for port forwarding of the REST calls of any
    function to a given port.

    Currently there are three constraints for applying this decorator.
    1. The function to be decorated must be defined within a class and not being a static method.
       The reason for that is because we need to be sure the function's signature will
       always include the class instance as its first argument.
    2. The url argument must be present as the second one in the positional argument list
       of the decorated function (right after the class instance argument).
    3. The url must follow the syntax specifications in RFC 1808:
       https://tools.ietf.org/html/rfc1808.html

    If all of the above constraints are fulfilled and the url is part of the
    urlMangleList, then the url is parsed and the port is substituted with the
    one provided as an argument to the decorator's wrapper function.

    param port: The port to which the REST call should be forwarded.
    """
    def portForwardDecorator(callFunc):
        """
        The actual decorator
        """
        urlMangleList = ['https://tivanov',
                         'https://alancc',
                         'https://cmsweb']

        def portMangle(callObj, url, *args, **kwargs):
            """
            Function used to check if the url coming with the current argument list
            is to be forwarded and if so change the port to the one provided as an
            argument to the decorator wrapper.

            :param classObj: This is the class object (slef from within the class)
                             which is always to be present in the signature of a
                             public method. We will never use this argument, but
                             we need it there for not breaking the positional
                             argument order
            :param url:      This is the actual url to be (eventually) forwarded
            :param *args:    The positional argument list coming from the original function
            :param *kwargs:  The keywords argument list coming from the original function
            """
            # As a first step try to get a logger from the calling object:
            if callable(getattr(callObj, 'logger', None)):
                logger = callObj.logger
            else:
                logger = logging.getLogger()

            forwarded = False
            try:
                oldUrl = urlparse(url)
                found = False
                if isinstance(url, str):
                    for mUrl in urlMangleList:
                        if url.startswith(mUrl):
                            netlocStr = u'%s:%d' % (oldUrl.hostname, port)
                            found = True
                            break
                elif isinstance(url, bytes):
                    for mUrl in urlMangleList:
                        if url.startswith(mUrl.encode('utf-8')):
                            netlocStr = b'%s:%d' % (oldUrl.hostname, port)
                            found = True
                            break
                if found:
                    newUrl = ParseResult(scheme=oldUrl.scheme,
                                         netloc=netlocStr,
                                         path=oldUrl.path,
                                         params=oldUrl.params,
                                         query=oldUrl.query,
                                         fragment=oldUrl.fragment)
                    newUrl = newUrl.geturl()
                    forwarded = True
            except Exception as ex:
                msg = "Failed to forward url: %s to port: %s due to ERROR: %s"
                logger.exception(msg, url, port, str(ex))
            if forwarded:
                return callFunc(callObj, newUrl, *args, **kwargs)
            else:
                return callFunc(callObj, url, *args, **kwargs)
        return portMangle
    return portForwardDecorator


class PortForward():
    """
    A class with a call method implementing a simple way to use the functionality
    provided by the protForward decorator as a pure functional call:
    EXAMPLE:
        from Utils.PortForward import PortForward

        portForwarder = PortForward(8443)
        url = 'https://cmsweb-testbed.cern.ch/couchdb'
        url = portForwarder(url)
    """
    def __init__(self, port):
        """
        The init method for the PortForward call class. This one is supposed
        to simply provide an initial class instance with a logger.
        """
        self.logger = logging.getLogger()
        self.port = port

    def __call__(self, url):
        """
        The call method for the PortForward class
        """
        def dummyCall(self, url):
            return url
        return portForward(self.port)(dummyCall)(self, url)
