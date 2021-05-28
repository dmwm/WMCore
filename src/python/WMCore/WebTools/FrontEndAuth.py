#!/usr/bin/env python

from future.utils import viewitems

import hashlib
import hmac

import cherrypy

from Utils.Utilities import lowerCmsHeaders


def get_user_info():
    """
    Helper function to return user based information of the request
    """
    return cherrypy.request.user


class FrontEndAuth(cherrypy.Tool):
    """
    Transparently allows a back-end cmsweb app to do
    authn/z based on the headers sent by the front-end.
    """

    def __init__(self, config):
        """Read hmac secret  and define cherrypy hook point."""
        # reads the bin key used to verify the hmac
        with open(config.key_file, "rb") as f:
            self.key = f.read()

        # Defines the hook point for cherrypy
        self._name = None
        self._point = 'before_request_body'
        self._priority = 60  # Just after the sessions being enabled

    def callable(self, role=None, group=None, site=None, authzfunc=None):
        """
        This is the method that is called in the cherrypy hook point (whenever
        the user-agent requests a page of the back-end app.
        """
        role = role or []
        group = group or []
        site = site or []
        # Sets initial user information for this request
        assert getattr(cherrypy.request, "user", None) is None
        cherrypy.request.user = {'dn': None,
                                 'method': None,
                                 'login': None,
                                 'name': None,
                                 'roles': {}}

        # Checks authn by reading front-end headers
        self.check_authentication()

        # Now checks authz
        self.check_authorization(role, group, site, authzfunc)

        # The user is authenticated and authorized. Then cherrypy will
        # call the proper app handler to show the requested page.
        # User info still available under cherrypy.thread_data.user.

    def check_authentication(self):
        """Read and verify the front-end headers, update the user
        dict with information about the authorized user."""
        headers = lowerCmsHeaders(cherrypy.request.headers)
        user = get_user_info()

        if 'cms-auth-status' not in headers:
            # Non SSL request
            raise cherrypy.HTTPError(403, "You are not allowed to access this resource.")

        if headers['cms-auth-status'] == 'NONE':
            # User authentication is optional
            return  # authn accepted

        # User information is available on headers
        prefix = suffix = ""
        hkeys = sorted(headers.keys())
        for hk in hkeys:
            hk = hk.lower()
            if hk[0:9] in ["cms-authn", "cms-authz"] and hk != "cms-authn-hmac":
                prefix += "h%xv%x" % (len(hk), len(headers[hk]))
                suffix += "%s%s" % (hk, headers[hk])
                hkname = hk.split('-', 2)[-1]
                if hk.startswith("cms-authn"):
                    user[hkname] = headers[hk]
                if hk.startswith("cms-authz"):
                    user['roles'][hkname] = {'site': set(), 'group': set()}
                    for r in headers[hk].split():
                        ste_or_grp, name = r.split(':')
                        user['roles'][hkname][ste_or_grp].add(name)

        vfy = hmac.new(self.key, prefix + "#" + suffix, hashlib.sha1).hexdigest()
        if vfy != headers["cms-authn-hmac"]:
            # HMAC does not match
            raise cherrypy.HTTPError(403, "You are not allowed to access this resource, hmac mismatch")

            # User authn accepted

    def check_authorization(self, role, group, site, authzfunc):
        """Format the authorization rules into lists and verify if the given
        user is allowed to access."""
        if authzfunc is None:
            authzfunc = self.defaultAuth

        # TOFIX: put role, group and site into canonical form

        # Turns arguments into lists
        if role and isinstance(role, str):
            role = [role]
        if group and isinstance(group, str):
            group = [group]
        if site and isinstance(site, str):
            site = [site]

        # Finally checks if the user is allowed
        if not authzfunc(get_user_info(), role, group, site):
            # Authorization denied
            raise cherrypy.HTTPError(403, "You are not allowed to access this resource, authz denied")

    def defaultAuth(self, user, role, group, site):
        """ Return True for authorized user, False otherwise.

        An user is authorized if he has any of the asked roles in the
        given sites or groups. When no roles is specified, belonging
        to any of the asked sites or groups is enough.
        """
        if not (role or group or site):
            return True

        for k, v in viewitems(user['roles']):
            if (not role) or (k in role):
                if not (group or site):
                    return True
                if set(group) & v['group']:
                    return True
                if set(site) & v['site']:
                    return True
        return False


class NullAuth(cherrypy.Tool):
    def __init__(self, config):
        # Defines the hook point for cherrypy
        self._name = None
        self._point = 'before_request_body'
        self._priority = 60  # Just after the sessions being enabled
        if cherrypy.server.environment == 'production':
            cherrypy.log.access_log.critical('You MUST NOT use the NullAuth in a production environment')
            raise cherrypy.CherryPyException('Invalid server authentication')
        else:
            cherrypy.log.access_log.warning("You are using the NullAuth, I hope you know what you're doing")

    def callable(self, role=None, group=None, site=None, authzfunc=None):
        role = role or []
        group = group or []
        site = site or []
        cherrypy.log.access_log.warning('NullAuth called for:')
        cherrypy.log.access_log.warning('\trole(s): %s \n\tgroup(s): %s \n\tsite(s): %s', role, group, site)

        if authzfunc:
            cherrypy.log.access_log.warning('\tusing authorisation function %s', authzfunc.__name__)
        cherrypy.request.user = {'dn': 'None',
                                 'method': 'Null Auth - totally insecure!',
                                 'login': 'fbloggs',
                                 'name': 'Fred Bloggs',
                                 'roles': {}}
