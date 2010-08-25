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

        # The full URL of this app (without mount points or views)
        # The app_url takes place of cherrypy.request.base
        if hasattr(config,'app_url'):
            self.app_url = config.app_url.rstrip('/') # without trailing '/'
        else:
            self.app_url = cherrypy.url()
        self.base_path = '%s/%s' % (self.app_url, self.config.mount_point.rstrip('/'))
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
        # It is allways allowed access to 'auth' URLs like the page that requests
        # the user login (pages under the config.mount_point)
        if cherrypy.request.script_name.startswith('/'+self.config.mount_point.rstrip('/')):
            return # skip config.mount_point pages

        # These methods are responsible for achieving auth and authz
        # before allowing access to any other page
        self.verify()
        self.process()
        self.check_authorization(role, group, site, authzfunc)
        
        # User authenticated and authorized. Now cherrypy calls the app handler to
        # show the requested page
    
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
        # We could force the url of the server here somehow, but it needs to be
        # unset if the user isn't logged in...
        openid_url = cherrypy.request.params.get('openid_url',None)        
        try:
            # ...so instead we assert that the url to be used is the one
            # configured
            if openid_url:
                assert openid_url == self.oidserver
        except:
            msg =  '<br><br><b>You are attempting to authenticate with %s. ' % openid_url
            msg += '<br>This is an invalid OpenID URL. You want %s.</b>' % self.oidserver
            cherrypy.session[self.session_name]['info'] = msg
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
        
        current_url = cherrypy.url(base=self.app_url)
        # If the user didn't inform his ID yet, redirect him to the login page
        if not openid_url:
            raise cherrypy.HTTPRedirect('%s?url=%s' % (self.login_path, current_url))
        #del cherrypy.request.params['openid_url']

        # Here it is where we start playing with OpenId
        oidconsumer = consumer.Consumer(self.get_session(), self.store)
        try:
            oidrequest = oidconsumer.begin(openid_url)
        except discover.DiscoveryFailure, exc:
            msg =  '<br><br><b>Could not connect to the OpenID server %s</b>' % openid_url
            msg += '<br><br> If you are running a private server instance, '
            msg += 'make sure it is running and its address is correct.'
            msg += '<br><br>Debug information:<br> %s' % cgi.escape(str(exc[0]))
            cherrypy.session[self.session_name]['info'] = msg
            raise cherrypy.HTTPRedirect(self.error_path)
        else:
            # Then finally the auth begins...
            # Set the return URL to be the one requested by the user
            return_to = current_url # was cherrypy.url(cp.path_info)
            trust_root = self.app_url # was cherrypy.request.base
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
            msg = ''
            if info.identity_url:
                msg += '<br><br><b>Could not authenticate %s</b>' % info.identity_url
            if info.message:
                msg += '<br><br><b>%s</b>' % info.message
            else:
                msg += '<br><br><b>Authentication failed for an unknown reason</b>'
            cherrypy.session[self.session_name]['info'] = msg
            raise cherrypy.HTTPRedirect(self.failed_path)
        elif info.status == consumer.CANCEL:
            # Indicates that the user cancelled the OpenID authentication request
            if info.identity_url:
                msg = '<br><br><b>The authentication for %s' % info.identity_url
                msg += ' was cancelled.</b>'
            else:
                msg =  '<br><br>Authentication failed due to one of '
                msg += 'the following reasons:<br>'
                msg += '<menu>'
                msg += '<li>The application %s' % self.app_url # cherrypy.request.base
                msg += ' is not registered to use the OpenID server;</li>'
                msg += '<li>Username and password did not verify;</li>'
                msg += '<li>The user cancelled the authentication.</li>'
                msg += '</menu>'
            cherrypy.session[self.session_name]['info'] = msg
            raise cherrypy.HTTPRedirect(self.cancel_path)
        elif info.status == consumer.SETUP_NEEDED:
            # Means that the request was in immediate mode and the server was
            # unable to authenticate the user without interaction.
            # So, should send the user to the server
            if info.setup_url: # maybe not available in OpenID 2.0
                raise cherrypy.HTTPRedirect(info.setup_url)
            msg = '<br><br><b>Could not authenticate without user interaction.</b>'
            cherrypy.session[self.session_name]['info'] = msg
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
            #raise cherrypy.HTTPRedirect(cherrypy.url(cherrypy.request.path_info))
            raise cherrypy.HTTPRedirect(current_url)

        # If the code reaches here, an unknown error happened
        msg = '<br><br><b>Invalid response from the server.</b>'
        cherrypy.session[self.session_name]['info'] = msg
        raise cherrypy.HTTPRedirect(self.error_path)

        # End of process()

    def check_authorization(self, role=[], group=[], site=[], authzfunc=None):
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
            msg =  '<br><br><b>You are not authorized to access '
            msg += '%s.</b>' % cherrypy.request.path_info
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
        
