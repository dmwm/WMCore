import time

import cherrypy
from cherrypy import Tool, request, tools

from WMCore.REST.Auth import RESTAuth


def set_request_time():
    """Utility to time stamp the start of HTTP request handling."""
    request.start_time = time.time()


def set_proxy_base(base=None):
    """Utility to correctly handle requests behind a proxy."""
    scheme = request.headers.get('X-Forwarded-Proto', request.base[:request.base.find("://")])
    base = request.headers.get('X-Forwarded-Host', base)
    if not base:
        port = cherrypy.request.local.port
        if port == 80:
            base = 'localhost'
        else:
            base = 'localhost:%s' % port

    base = base.split(',')[0].strip()
    if base.find("://") == -1:
        base = scheme + "://" + base
    request.base = base

    xff = request.headers.get('X-Forwarded-For')
    if xff:
        xff = xff.split(',')[0].strip()
        request.remote.ip = xff


tools.cms_auth = RESTAuth()
tools.time = Tool('on_start_resource', set_request_time)
tools.proxy = Tool('before_request_body', set_proxy_base, priority=30)
