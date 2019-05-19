import cherrypy
import hashlib
import hmac
import re

from Utils.Utilities import lowerCmsHeaders

def get_user_info():
    "Helper function to return user based information of the request"
    return cherrypy.request.user


def user_info_from_headers(key, verbose=False):
    """Read the user information HTTP request headers added by front-end.
    Validates the HMAC on them to check for tampering, and if all is ok,
    returns user info object with the data from the headers."""
    # Set initial user information for this request
    log = cherrypy.log
    headers = lowerCmsHeaders(cherrypy.request.headers)
    user = {'dn': None, 'method': None, 'login': None, 'name': None, 'roles': {}}

    # Reject if request was not authenticated.
    if 'cms-auth-status' not in headers:
        log("ERROR: authz denied, front-end headers not present")
        raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")

    # If authentication is optional and wasn't done, accept.
    if headers['cms-auth-status'] == 'NONE':
        if verbose: log("DEBUG: authn optional and missing")
        return

    # Extract user information from the headers. Collect data required
    # for HMAC validation while processing headers.
    prefix = suffix = ""
    hkeys = sorted(headers.keys())
    for hk in hkeys:
        hk = hk.lower()
        if hk[0:9] in ("cms-authn", "cms-authz") and hk != "cms-authn-hmac":
            prefix += "h%xv%x" % (len(hk), len(headers[hk]))
            suffix += "%s%s" % (hk, headers[hk])
            hkname = hk.split('-', 2)[-1]
            if hk.startswith("cms-authn"):
                val = headers[hk]
                if hk in ("cms-authn-name", "cms-authn-dn"):
                    val = unicode(val, "utf-8")
                user[hkname] = val
            if hk.startswith("cms-authz"):
                user['roles'][hkname] = {'site': set(), 'group': set()}
                for r in headers[hk].split():
                    site_or_group, name = r.split(':')
                    user['roles'][hkname][site_or_group].add(name)

    # Check HMAC over authn/z headers with server key. If differs, reject.
    cksum = hmac.new(key, prefix + "#" + suffix, hashlib.sha1).hexdigest()
    if cksum != headers["cms-authn-hmac"]:
        log("ERROR: authz hmac mismatch, %s vs. %s" % (cksum, headers["cms-authn-hmac"]))
        raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")

    # Authn/z is legal, accept
    if verbose:
        log("DEBUG: authn accepted for user %s" % user)
    return user


def authz_canonical(val):
    """Make a name canonical."""
    return re.sub(r"[^a-z0-9]+", "-", val.lower())


def authz_match(role=None, group=None, site=None, verbose=False):
    """Match user against authorisation requirements."""
    role = role or []
    group = group or []
    site = site or []
    user = get_user_info()
    log = cherrypy.log

    # If role, group or site are strings, convert to list first.
    role = (role and isinstance(role, str) and [role]) or role
    group = (group and isinstance(group, str) and [group]) or group
    site = (site and isinstance(site, str) and [site]) or site

    # Reformat all items into canonical format.
    role = role and list(map(authz_canonical, role))
    group = group and list(map(authz_canonical, group))
    site = site and list(map(authz_canonical, site))

    # If role, group and site are all empty, no authz requirements: pass
    if not (role or group or site):
        if verbose:
            log("DEBUG: authz accepted nil requirements for user %s" % user)
        return

    # Otherwise determine set intersection of requirements.
    for r, authz in ((user and user['roles']) or {}).iteritems():
        if (not role) or (r in role):
            if not (group or site):
                if verbose:
                    log("DEBUG: authz accepted role '%s' for user %s" % (r, user))
                return
            if set(group) & authz['group']:
                if verbose:
                    log("DEBUG: authz accepted role '%s' group %s for user %s" % (r, group, user))
                return
            if set(site) & authz['site']:
                if verbose:
                    log("DEBUG: authz accepted role '%s' site %s for user %s" % (r, site, user))
                return

    # Deny access, requirements weren't fulfilled
    log("ERROR: authz denied role %s group %s site %s for user %s" % (role, group, site, user))
    raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")


def authz_user(role=None, group=None, site=None, key=None, verbose=False):
    """Default authorisation implementation: returns True for an authorised
    user, one with any of the requested roles for the given sites or groups.

    If no roles are specified, belonging to any of the requested sites or
    groups is sufficient."""
    role = role or []
    group = group or []
    site = site or []
    # Get user information from request headers, then run real matcher.
    assert getattr(cherrypy.request, "user", None) == None
    cherrypy.request.user = user_info_from_headers(key, verbose)
    authz_match(role, group, site, verbose)


_fake_warned = False


def authz_fake(role=None, group=None, site=None, verbose=False):
    """Fake authorisation routine."""
    role = role or []
    group = group or []
    site = site or []
    log = cherrypy.log
    global _fake_warned
    if not _fake_warned:
        if cherrypy.server.environment == 'production':
            log("ERROR: authz faking denied in production server")
            raise Exception("authz faking denied in production server")
        if cherrypy.server.socket_host not in ("127.0.0.1", "::1"):
            log("ERROR: authz faking denied on non-local interface")
            raise Exception("authz faking denied on non-local interface")
        log("WARNING: authz faking activated, only test use is permitted")
        _fake_warned = True
    cherrypy.request.user = {
        'method': 'fake',
        'login': 'fake_insecure_auth',
        'name': 'Fake Insecure Auth',
        'dn': '/CN=Fake Insecure Auth',
        'roles': {'fake': {'site': set(['fake']), 'group': set(['fake'])}}
    }

    if verbose:
        log("DEBUG: authz accepted bypass role %s group %s site %s for user %s"
            % (role, group, site, cherrypy.request.user))


class RESTAuth(cherrypy.Tool):
    """Restrict access based on CMSWEB authn/z information."""
    _key = None

    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_request_body', authz_user, priority=60)

    def _setup(self):
        """Hook this tool into cherrypy request."""
        log = cherrypy.log
        hooks = cherrypy.request.hooks
        conf = self._merged_args()

        # Install authz policy. Load front-end hmac key on first use.
        prio = conf.pop("priority", self._priority)
        policy = conf.pop("policy", "user")
        if policy == "user":
            key_file = conf.pop("key_file")
            if not self._key:
                with open(key_file, "rb") as fd:
                    self._key = fd.read()
            conf["key"] = self._key

            hooks.attach(self._point, self.callable, priority=prio, **conf)
        elif policy == "dangerously_insecure":
            conf.pop("key_file", None)
            hooks.attach(self._point, authz_fake, priority=prio, **conf)
        else:
            raise Exception('unknown authz policy "%s"' % policy)
