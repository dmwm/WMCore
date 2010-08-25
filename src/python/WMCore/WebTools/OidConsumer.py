#!/usr/bin/env python

import cherrypy
import openid
from openid.consumer import consumer, discover
from openid.cryptutil import randomString # To generate session IDs
from openid.store import filestore
import cms_sreg as sreg # To request authorization data
import cgi # To use escape()
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
# default handler for login/etc pages
from WMCore.WebTools.OidDefaultHandler import OidDefaultHandler

# Define session states
UNKNOWN = 0        # Does not know about the authentication state
PROCESSING = 1     # Waiting for the oid server response about the auth
AUTHENTICATED = 2  # User authenticated correctly

DEFAULT_SESSION_NAME = 'SecurityModule'
DEFAULT_OID_SERVER = 'https://cmsweb.cern.ch/security/'
#-----------------------------------------------------------------------------
# A class to transparently transform a cherrypy-based web app into an OpenID
# consumer, thus allowing it to use CERN auth/authorization facilities
class OidConsumer(cherrypy.Tool):
    def __init__(self, config):
        self.config = config
        self.decoder = json.JSONDecoder()
        
        if self.config.store == 'filestore':
            self.store = filestore.FileOpenIDStore(self.config.store_path)
        
        
        self.session_name = getattr(self.config, 'session_name', DEFAULT_SESSION_NAME)
        self.oidserver = getattr(self.config, 'oid_server', DEFAULT_OID_SERVER)

        self.base_path = '%s/%s' % (cherrypy.url(), self.config.mount_point)
        self.login_path = '%s/login' % self.base_path
        self.failed_path = '%s/failure' % self.base_path
        self.cancel_path = '%s/cancelled' % self.base_path
        self.error_path = '%s/error' % self.base_path
        self.logout_path = '%s/logout' % self.base_path
        self.authz_path = '%s/authz' % self.base_path
        self.authdummy_path = '%s/dummy' % self.base_path
        self.defhandler = OidDefaultHandler(config)

        # Defines the hook point for cherrypy
        self._name = None
        self._point = 'before_request_body'
        self._priority = 60 # Just after the sessions being enabled

    def callable(self, role=[], group=[], site=[], authzfunc=None):
        """
        This is the method that is called in the cherrypy hook point (whenever
        the user-agent requests a page
        """
        self.verify()
        if isinstance(cherrypy.request.handler,cherrypy._cpdispatch.LateParamPageHandler):
            self.process()
            self.check_authorization(role, group, site, authzfunc)
            self.defaults()
        # Now cherrypy calls the handler()
    
    def get_session(self):
        oidsession = cherrypy.session.get(self.session_name, None)

        if not oidsession or not isinstance(oidsession, dict):
            oidsession = {}
        
        if 'sid' not in oidsession:
            sid = randomString(16, '0123456789abcdef')
            oidsession['sid'] = sid
            cherrypy.session[self.session_name] = oidsession
            cherrypy.session[self.session_name]['status'] = UNKNOWN

        return cherrypy.session[self.session_name]

    def is_processing(self):
        if cherrypy.session.has_key(self.session_name):
            if 'status' in cherrypy.session[self.session_name]:
                if cherrypy.session[self.session_name]['status'] in [PROCESSING, AUTHENTICATED]:
                    return True
        return False

    def is_authenticated(self):
        if cherrypy.session.has_key(self.session_name):
            if 'status' in cherrypy.session[self.session_name]:
                if cherrypy.session[self.session_name]['status'] == AUTHENTICATED:
                    return True
        return False

    def is_relogin(self, openid_url):
        if openid_url:
            if cherrypy.session.has_key(self.session_name):
                if 'openid_url' in cherrypy.session[self.session_name]:
                    if cherrypy.session[self.session_name]['openid_url'] != openid_url:
                        return True
        return False

    def verify(self):
        current_url = cherrypy.request.script_name or '/'
        # Do not verify auth URLs like the page that requests the user login
        if cherrypy.request.path_info.startswith(self.base_path):
            return

        # We could force the url of the server here somehow, but it needs to be
        # unset if the user isn't logged in...
        openid_url = cherrypy.request.params.get('openid_url',None)
        
        try:
            # ...so instead we assert that the url to be used is the one
            # configured
            if openid_url:
                assert openid_url == self.oidserver
        except:
            msg = 'Error in discovery: wrong OpenID server '
            msg += 'You are attempting to authenticate with %s. ' % openid_url
            msg += 'This is an invalid OpenID URL. You want %s.' % self.oidserver
            session = self.get_session()
            session['info'] = msg
            raise cherrypy.HTTPRedirect(self.error_path)

        # If the user requested to login again without logging out first,
        # force a logout
        if self.is_relogin(openid_url):
            # *** Should call cherrypy.lib.sessions.expire() and reprocess
            # the request instead of just deleting the session dict ***
            del cherrypy.session[self.session_name] # logout before continuing

        # Do not start the verification process if it is was already started
        if self.is_processing():
            return

        # If the user didn't inform his ID yet, redirect him to the login page
        if not openid_url:
            raise cherrypy.HTTPRedirect('%s?url=%s' % (self.login_path, current_url))
        #del cherrypy.request.params['openid_url']

        # Here it is where we start playing with OpenId
        oidconsumer = consumer.Consumer(self.get_session(), self.store)
        try:
            oidrequest = oidconsumer.begin(openid_url)
        except discover.DiscoveryFailure, exc:
            msg = 'Error in discovery: %s' % cgi.escape(str(exc[0]))
            msg += 'Check that you correctly provided the OpenID server in '
            msg += 'your OpenID URL.'
            cherrypy.session[self.session_name]['info'] = msg
            raise cherrypy.HTTPRedirect(self.error_path)
        else:
            # Then finally the auth begins...
            # Set the return URL to be the one requested by the user
            return_to = cherrypy.url(cherrypy.request.path_info)
            trust_root = cherrypy.request.base
            cherrypy.session[self.session_name]['return_to'] = return_to
            cherrypy.session[self.session_name]['status'] = PROCESSING


            # Extends the OpenID request using SREG. Uses it to get authoriztion
            # data. Since this extension makes part of the original OpenID
            # request, it will be sent securely.
            sreg_request = sreg.SRegRequest(required=['permissions',
                                                      'fullname',
                                                      'dn'])
            oidrequest.addExtension(sreg_request)

            # Should this auth request be sent as HTTPRedirect or as a POST?
            if oidrequest.shouldSendRedirect():
                # redirectURL() encodes the OpenID request into an URL
                redirect_url = oidrequest.redirectURL(trust_root, return_to)
                # Redirects the user-agent to the oid server using the
                # encoded URL. After authenticating the user, the oid server
                # will redirect the user agent back to 'return_to'
                raise cherrypy.HTTPRedirect(redirect_url)
            else:
                form_html = oidrequest.htmlMarkup(
                    trust_root, return_to,
                    form_tag_attrs={'id':'openid_message'})
                cherrypy.session[self.session_name]['info'] = form_html
                raise cherrypy.HTTPRedirect(self.authdummy_path)
        # End of verify()

    def process(self):
        """
        This function deals with the oid server response (when it redirects
        the user-agent back to here. This request contains the status of the
        user authentication in the oid server
        """
        # Do not process auth URLs like the page that requests the user login
        if cherrypy.request.path_info.startswith(self.base_path):
            return

        # Also ignores if the authentication process already completed
        if self.is_authenticated():
            return
        
        oidconsumer = consumer.Consumer(self.get_session(), self.store)
        cherrypy.session[self.session_name]['status'] = UNKNOWN

        # Ask the oid library to verify the response received from the oid serv.
        current_url=cherrypy.session[self.session_name].get('return_to',None)
        info = oidconsumer.complete(cherrypy.request.params,current_url)
        # Now verifies what it does mean
        if info.status == consumer.FAILURE:
            # The OpenID protocol failed, either locally or remotely
            cherrypy.session[self.session_name]['info'] = info.identity_url
            raise cherrypy.HTTPRedirect(self.failed_path)
        elif info.status == consumer.CANCEL:
            # Indicates that the user cancelled the OpenID authentication request
            cherrypy.session[self.session_name]['info'] = info.identity_url
            raise cherrypy.HTTPRedirect(self.cancel_path)
        elif info.status == consumer.SETUP_NEEDED:
            # Means that the request was in immediate mode and the server was
            # unable to authenticate the user without interaction.
            # So, should send the user to the server
            if info.setup_url: # maybe not available in OpenID 2.0
                raise cherrypy.HTTPRedirect(info.setup_url)
            cherrypy.session[self.session_name]['info'] = info.identity_url
            raise cherrypy.HTTPRedirect(self.failed_path)
        elif info.status == consumer.SUCCESS:
            if info.endpoint.canonicalID:
                # Should use canonicalIDs instead of user-friendly oid URLs,
                # but leave it like this now
                pass

            # Gets additional information that came in the server response.
            # The authorization information is supposed to come in here.
            sreg_data = sreg.SRegResponse.fromSuccessResponse(info) or {}
            for i in ['fullname', 'dn']:
                cherrypy.session[self.session_name][i] = sreg_data.get(i,None)
            # Should do a better job when passing a dict as a string
            cherrypy.session[self.session_name]['permissions'] = \
                                   eval(sreg_data.get('permissions',"{}"))
            
            # Set the new session state to authenticated
            cherrypy.session[self.session_name]['status'] = AUTHENTICATED
            cherrypy.session[self.session_name]['openid_url'] = \
                                      info.getDisplayIdentifier()
            cherrypy.request.params = {} # empty request parameters

            # Finally redirects the user-agent to the URL initially requested
            raise cherrypy.HTTPRedirect(cherrypy.url(cherrypy.request.path_info))

        # If the code reaches here, an unknown error happened
        cherrypy.session[self.session_name]['info'] = 'unknown reason'
        raise cherrypy.HTTPRedirect(self.error_path)

        # End of process()

    def check_authorization(self, role=[], group=[], site=[], authzfunc=None):
        # Do not process auth URLs like the page that requests the user login
        if cherrypy.request.path_info.startswith(self.base_path):
            return
        if authzfunc == None:
            authzfunc = self.defaultAuth

        # Now prepares arguments to pass to the authzfunc
        permissions = cherrypy.session[self.session_name]['permissions']
        if type(permissions) == type('str'):
            permissions = self.decoder.decode(permissions)
        user = {
                'permissions':permissions,
                'fullname': cherrypy.session[self.session_name]['fullname'],
                'dn': cherrypy.session[self.session_name]['dn']}

        # For arguments to be lists
        if role and not isinstance(role, list):
            role = [role]
        if group and not isinstance(group, list):
            group = [group]
        if site and not isinstance(site, list):
            site = [site]

        # Finally checks if the user is allowed
        if not authzfunc(user, role, group, site):
            # Not allowed
            msg = 'You are not allowed to access %s' % cherrypy.request.path_info
            cherrypy.session[self.session_name]['info'] = msg
            raise cherrypy.HTTPRedirect(self.authz_path)
        
    def defaultAuth(self, user, role=[], group=[], site=[]):
        """
        A default authorisation function. Returns True/False. It checks that the
        user has the roles for either the groups or sites specified. If the site
        or group list is 0 length it just checks the user has the named role. If
        there is no role specified check that user is a non-empty dict. 
        
        Other authorisation functions should have the same signature and behave 
        in the same manner.

        This code is only reached if the user is already authenticated 
        """
        assert(user) # user shouldn't be empty here
        if role == [] and group == [] and site == []:
            return True  # Allow if no authorization requeriment is present

        if len(role):
            # one or more roles specified 
            for r in role:
                if r in user['permissions'].keys():
                    if len(group) == 0 and len(site) == 0:
                        return True
                    for g in group:
                        if g in user['permissions'][r]:
                            return True
                    for s in site:
                        if s in user['permissions'][r]:
                            return True
        else:
            # no role specified
            l = []
            for i in user['permissions'].values():
                if type(i) == type([]):
                    l.extend(i)
                else:
                    l.append(i)
            for s in site:
                if s in l:
                    return True
            for g in group:
                if g in l:
                    return True
   
        return False
        
    def defaults(self):
        if not cherrypy.request.path_info.startswith(self.base_path):
            return # We only need to worry about handlers for the auth path

        # Perhaps there is another (safe) way to check if a handler was found
        if isinstance(cherrypy.request.handler,cherrypy._cpdispatch.LateParamPageHandler):
            return # Ok. A handler was found

        if cherrypy.request.path_info.startswith(self.login_path):
            cherrypy.request.handler = self.defhandler.login
        elif cherrypy.request.path_info.startswith(self.logout_path):
            cherrypy.request.handler = self.defhandler.logout
        elif cherrypy.request.path_info.startswith(self.failed_path):
            cherrypy.request.handler = self.defhandler.failure
        elif cherrypy.request.path_info.startswith(self.cancel_path):
            cherrypy.request.handler = self.defhandler.cancelled
        elif cherrypy.request.path_info.startswith(self.error_path):
            cherrypy.request.handler = self.defhandler.error
        else:
            pass # don't know how to handle it at all
