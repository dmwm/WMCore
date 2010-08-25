#!/usr/bin/env python

import cherrypy
import openid
from openid.consumer import consumer, discover
from openid.cryptutil import randomString # To generate session IDs
from openid.store import filestore
import cms_sreg as sreg # To request authorization data
import cgi # To use escape()
import urllib # To use urlencode()
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
        
        self.sessname = getattr(self.config, 'session_name', DEFAULT_SESSION_NAME)
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
        if self.session()['status'] in [UNKNOWN]:
            self.request_auth()
        if self.session()['status'] in [PROCESSING]:
            self.verify_auth()

        # 'status' is AUTHENTICATED, now checks if the user is authorized
        self.check_authorization(role, group, site, authzfunc)
        
        # End of callable(). The user authenticated and authorized.
        # Now cherrypy calls the app handler to show the requested page.
    
    def request_auth(self):
        """
        This method generates an signed openid request and redirects the user
        to the oid server to process it.
        """
        # Here it is where we start playing with OpenId
        oidconsumer = consumer.Consumer(self.session(), self.store)
        try:
            oidrequest = oidconsumer.begin(self.oidserver)
        except discover.DiscoveryFailure, exc:
            msg =  '<br><br><b>Could not connect to the OpenID server %s</b>' % self.oidserver
            msg += '<br><br> If you are running a private server instance, '
            msg += 'make sure it is running and its address is correct.'
            msg += '<br><br>Debug information:<br> %s' % cgi.escape(str(exc[0]))
            self.session()['info'] = msg
            raise cherrypy.HTTPRedirect(self.error_path)
            #raise cherrypy.HTTPRedirect(self.error_path+'?msg='+urllib.quote_plus(msg))
        else:
            # Then finally the auth begins...
            # Set the return URL to be the one requested by the user
            current_url = cherrypy.url(base=self.app_url)
            return_to = current_url # was cherrypy.url(cp.path_info)
            trust_root = self.app_url # was cherrypy.request.base
            self.session()['return_to'] = return_to
            self.session()['status'] = PROCESSING

            # Extends the OpenID request using SREG. Uses it to get authoriztion
            # data. Since this extension makes part of the original OpenID
            # request, it will be sent securely.
            sreg_request = sreg.SRegRequest(required=['permissions',
                                                      'fullname',
                                                      'dn'])
            oidrequest.addExtension(sreg_request)

            # redirectURL() encodes the OpenID request into an URL
            redirect_url = oidrequest.redirectURL(trust_root, return_to)
            # Redirects the user-agent to the oid server using the
            # encoded URL. After authenticating the user, the oid server
            # will redirect the user agent back to 'return_to'
            raise cherrypy.HTTPRedirect(redirect_url)

        # End of verify()

    def verify_auth(self):
        """
        This method deals with the oid server response (when it redirects
        the user-agent back to here. This request contains the status of the
        user authentication with the oid server
        """
        self.session()['status'] = UNKNOWN

        # Ask the oid library to verify the response received from the oid serv.
        current_url=self.session()['return_to']
        #current_url=cherrypy.url(base=self.app_url)
        oidconsumer = consumer.Consumer(self.session(), self.store)
        info = oidconsumer.complete(cherrypy.request.params, current_url)

        # Now verifies what the oid server response means
        if info.status == consumer.SUCCESS:
            if info.endpoint.canonicalID:
                # Should use canonicalIDs instead of user-friendly oid URLs,
                # but leave it like this now
                pass

            # Gets additional information that came in the server response.
            # The authorization information is supposed to come in here.
            sreg_data = sreg.SRegResponse.fromSuccessResponse(info) or {}
            for i in ['fullname', 'dn']:
                self.session()[i] = sreg_data.get(i,None)
            # Should do a better job when passing a dict as a string
            self.session()['permissions'] = eval(sreg_data.get('permissions',"{}"))
            
            # Set the new session state to authenticated
            self.session()['status'] = AUTHENTICATED
            self.session()['user_url'] = info.getDisplayIdentifier()
            #cherrypy.request.params = {} # empty request parameters

            # Finally redirects the user-agent to the URL initially requested
            raise cherrypy.HTTPRedirect(current_url)

        elif info.status == consumer.FAILURE:
            # The OpenID protocol failed, either locally or remotely
            msg = ''
            if info.identity_url:
                msg += '<br><br><b>Could not authenticate %s</b>' % info.identity_url
            if info.message:
                msg += '<br><br><b>%s</b>' % info.message
            else:
                msg += '<br><br><b>Authentication failed for an unknown reason</b>'
            self.session()['info'] = msg
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
            self.session()['info'] = msg
            raise cherrypy.HTTPRedirect(self.cancel_path)

        elif info.status == consumer.SETUP_NEEDED:
            # Means that the request was in immediate mode and the server was
            # unable to authenticate the user without interaction.
            # So, should send the user to the server
            if info.setup_url: # maybe not available in OpenID 2.0
                raise cherrypy.HTTPRedirect(info.setup_url)
            msg = '<br><br><b>Could not authenticate without user interaction.</b>'
            self.session()['info'] = msg
            raise cherrypy.HTTPRedirect(self.failed_path)

        # If the code reaches here, an unknown error happened
        msg = '<br><br><b>Invalid response from the server.</b>'
        self.session()['info'] = msg
        raise cherrypy.HTTPRedirect(self.error_path)

        # End of process()

    def check_authorization(self, role=[], group=[], site=[], authzfunc=None):
        if authzfunc == None:
            authzfunc = self.defaultAuth

        # Now prepares arguments to pass to the authzfunc
        permissions = self.session()['permissions']
        if type(permissions) == type('str'):
            permissions = self.decoder.decode(permissions)
        user = {
                'permissions':permissions,
                'fullname': self.session()['fullname'],
                'dn': self.session()['dn']}

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
            self.session()['info'] = msg
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
        

    def session(self):
        """
        Uses cherrypy sessions instead of implementing by my own because:
        - The sessionid is bounded to the user-agent and then less subject
        to sessionid hijacking (when the cookie is theft or the sessionid
        is guessed)
        - It has a protection against session fixation attacks
        (see http://en.wikipedia.org/wiki/Session_fixation)
        - It allows me to choose the backend to store session information
    
        Another more secure solution to consider would be to use the SSL/TLS
        session identifier. But it would require changing the frontend config
        to set the SSL_SESSION_ID variable into the request sent to the backend
        """
        oidsession = cherrypy.session.get(self.sessname, None)
        if not oidsession:
            cherrypy.session[self.sessname] = {}
            cherrypy.session[self.sessname]['sid'] = randomString(16,'0123456789abcdef')
            cherrypy.session[self.sessname]['status'] = UNKNOWN  # auth state of this session
            cherrypy.session[self.sessname]['user_url'] = None # The user related to this session
                                                               # user_url = self.oidserver+'id/'+the real
                                                               #            username seen by the oid server
                                                               #            (will come from Hypernews/SiteDB)
            cherrypy.session[self.sessname]['info'] = None # no additonal info
            cherrypy.session[self.sessname]['return_to'] = None
            cherrypy.session[self.sessname]['fullname'] = None
            cherrypy.session[self.sessname]['dn'] = None
            cherrypy.session[self.sessname]['permissions'] = None

        return cherrypy.session[self.sessname]

#-------------------------------------------------------------------------------

def quoteattr(nonhtmltext):
    'Prepares a text string to be used with HTML'
    htmltext = cgi.escape(nonhtmltext, 1)
    return '"%s"' % (htmltext,)
