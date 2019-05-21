# pylint: disable-msg=C0103,C0111,W0212
"""
Throttled module defines necessary classes to throttle context.
Should be used in Web server APIs which wants to throttle clients.
"""

from __future__ import division

# standard modules
import time
import threading

# cherrypy modules
import cherrypy


class _ThrottleCounter(object):
    """
    _ThrottleCounter class define throttle parameter and
    enter/exit methods to work with `with` context.
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
            raise cherrypy.HTTPError("500 Internal Server Error", msg)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        "Define exit method for `with` context"
        pass


class UserThrottle(object):
    """
    UserThrottle class defines how to handle throttle mechanism.
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
        Define throttle context via _ThrottleCounter class
        which will count number of requests given user made in trange
        :param user: user name
        :param trange: time range while keep user access history
        """
        return _ThrottleCounter(self, user, trange)

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
            self.users_time.setdefault(user, time.time())
            last_time = self.users_time[user]
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

global_user_throttle = UserThrottle()
