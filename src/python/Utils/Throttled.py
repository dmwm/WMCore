# pylint: disable-msg=C0103,C0111,W0212
"""
Throttled module defines necessary classes to throttle context.
Should be used in Web server APIs which wants to throttle clients.
"""

from __future__ import division

# standard modules
from builtins import object
import logging
import threading

# cherrypy modules
import cherrypy


class _ThrottleCounter(object):
    """
    _ThrottleCounter class define throttle parameter and
    enter/exit methods to work with `with` context.
    """

    def __init__(self, throttle, user, debug=False):
        self.throttle = throttle
        self.user = user
        self.debug = debug

    def __enter__(self):
        "Define enter method for `with` context"
        ctr = self.throttle._incUser(self.user)
        if self.debug:
            msg = "Entering throttled function with counter %d for user %s" \
                    % (ctr, self.user)
            self.throttle.logger.debug(msg)
        if ctr >= self.throttle.getLimit():
            self.throttle._decUser(self.user)
            msg = "The current number of active operations for this resource"
            msg += " exceeds the limit of %d for user %s" \
                   % (self.throttle.getLimit(), self.user)
            raise cherrypy.HTTPError("500, Internal Server Error", msg)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        "Define exit method for `with` context"
        ctr = self.throttle._decUser(self.user)
        if self.debug:
            msg = "Exiting throttled function with counter %d for user %s" \
                  % (ctr, self.user)
            self.throttle.logger.debug(msg)


class UserThrottle(object):
    """
    UserThrottle class defines how to handle throttle mechanism.
    """

    def __init__(self, limit=3):
        self.lock = threading.Lock()
        self.tls = threading.local()
        self.users = {}
        self.limit = limit
        self.logger = logging.getLogger("WMCore.UserThrottle")

    def getLimit(self):
        "Return throttle limit"
        return self.limit

    def throttleContext(self, user, debug=False):
        "defint throttle context"
        self.users.setdefault(user, 0)
        return _ThrottleCounter(self, user, debug)

    def make_throttled(self, debug=False):
        """
        decorator for throttled context
        """
        def throttled_decorator(fn):
            """
            A decorator
            """
            def throttled_wrapped_function(*args, **kw):
                """
                A wrapped function.
                """
                username = cherrypy.request.user.get('login', 'Unknown') \
                        if hasattr(cherrypy.request, 'user') else 'Unknown'
                with self.throttleContext(username, debug):
                    return fn(*args, **kw)
            return throttled_wrapped_function
        return throttled_decorator

    def _incUser(self, user):
        "increment user count"
        retval = 0
        with self.lock:
            retval = self.users[user]
            if getattr(self.tls, 'count', None) is None:
                self.tls.count = 0
            self.tls.count += 1
            if self.tls.count == 1:
                self.users[user] = retval + 1
        return retval

    def _decUser(self, user):
        "decrecrement user count"
        retval = 0
        with self.lock:
            retval = self.users[user]
            self.tls.count -= 1
            if self.tls.count == 0:
                self.users[user] = retval - 1
        return retval

global_user_throttle = UserThrottle()
