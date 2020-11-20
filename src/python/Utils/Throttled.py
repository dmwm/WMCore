# pylint: disable-msg=C0103,C0111,W0212
"""
Throttled module defines necessary classes to throttle context.
Should be used in Web server APIs which wants to throttle clients.

The counter based approach, implemented in _ThrottleCounter class,
defines throttling on total number of user access to the underlying
app. But its counter depends on time execution of the API.

The time based approach, implemented in _ThrottleTimeCount class,
count total number of user accesses within specified time interval
regardless of time execution of the underlying API(s).

Here is an example how to use this module/classes:

# counter based throttline

from Utils.Throttling import UserThrottle
thr = UserThrottle(limit=5) # adjust threshold limit here
@thr.make_throttled()
def api():
    # define your api logic here

# time based throttling

from Utils.Throttling import UserThrottleTime
thr = UserThrottleTime(limit=5) # adjust threshold limit here
@thr.make_throttled(trange=2) # adjust trange (in sec) here
def api():
    # define your api logic here

"""

from __future__ import division

# standard modules
import time
from builtins import object
import logging
import threading

# cherrypy modules
import cherrypy


class _ThrottleCounter(object):
    """
    _ThrottleCounter class defines throttle parameter and
    enter/exit methods to work with `with` context.
    """

    def __init__(self, throttle, user, debug=False):
        self.throttle = throttle
        self.user = user
        if debug:
            self.throttle.logger.setLevel(logging.DEBUG)

    def __enter__(self):
        "Define enter method for `with` context"
        ctr = self.throttle._incUser(self.user)
        msg = "Entering throttled function with counter %d for user %s" \
                % (ctr, self.user)
        self.throttle.logger.debug(msg)
        if ctr > self.throttle.getLimit():
            msg = "The current number of active operations for this resource"
            msg += " exceeds the limit of %d for user %s" \
                   % (self.throttle.getLimit(), self.user)
            raise cherrypy.HTTPError("429 Too Many Requests", msg)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        "Define exit method for `with` context"
        ctr = self.throttle._decUser(self.user)
        msg = "Exiting throttled function with counter %d for user %s" \
                % (ctr, self.user)
        self.throttle.logger.debug(msg)

class _ThrottleTimeCounter(object):
    """
    _ThrottleTimeCounter class defines time range throttled mechanism
    """

    def __init__(self, throttle, user, trange):
        self.throttle = throttle
        self.user = user
        self.trange = trange

    def __enter__(self):
        "Define enter method for `with` context"
        ctr = self.throttle._incUser(self.user, self.trange)
        if ctr > self.throttle.getLimit():
            self.throttle._decUser(self.user)
            msg = "The current number of active operations for this resource"
            msg += " exceeds the limit of %d for user %s in last %s sec" \
                   % (self.throttle.getLimit(), self.user, self.trange)
            raise cherrypy.HTTPError("429 Too Many Requests", msg)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        "Define exit method for `with` context"
        pass


class UserThrottleTime(object):
    """
    UserThrottle class defines how to handle throttle time range based mechanism.
    """

    def __init__(self, limit=3):
        self.lock = threading.Lock()
        self.users = {}
        self.users_time = {}
        self.limit = limit

    def getLimit(self):
        "Return throttle limit"
        return self.limit

    def throttleContext(self, user, trange=60):
        """
        Define throttle context via _ThrottleTimeCounter class
        which will count number of requests given user made in trange
        :param user: user name
        :param trange: time range while keep user access history
        """
        return _ThrottleTimeCounter(self, user, trange)

    def make_throttled(self, trange=60):
        "decorator for throttled context"
        def throttled_decorator(fn):
            def throttled_wrapped_function(*args, **kw):
                username = cherrypy.request.user.get('login', 'Unknown') \
                        if hasattr(cherrypy.request, 'user') else 'Unknown'
                with self.throttleContext(username, trange):
                    return fn(*args, **kw)
            return throttled_wrapped_function
        return throttled_decorator

    def reset(self, user):
        "Reset user activities, i.e. access counter and last time of access"
        self.users[user] = 0
        self.users_time[user] = time.time()

    def _incUser(self, user, trange=60):
        "increment user count"
        with self.lock:
            self.users.setdefault(user, 0)
            last_time = self.users_time.setdefault(user, time.time())
            # increase counter within our trange
            if abs(time.time()-last_time) < trange:
                self.users[user] += 1
            else:
                self.reset(user)
        return self.users[user]

    def _decUser(self, user, trange=60):
        "decrecrement user count"
        with self.lock:
            # decrease counter outside of our trange
            last_time = self.users_time[user]
            if abs(time.time()-last_time) < trange:
                # gradually decrease user counter based on his/her elapsed time
                step = int(trange - abs(time.time()-last_time))
                self.users[user] -= step
                if self.users[user] < 0:
                    self.reset(user)
            else:
                self.reset(user)

class UserThrottle(object):
    """
    UserThrottle class defines throttle counter based mechanism.
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
