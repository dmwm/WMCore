#!/usr/bin/env python

import cherrypy
import openid
from openid.consumer import consumer, discover
from openid.store import filestore
import cms_sreg as sreg # To request authorization data
import cgi # To use escape()
#import urllib # To use urlencode()
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
# default handler for error/cancel/logout/etc pages
from WMCore.WebTools.OidDefaultHandler import OidDefaultHandler

# Define session states
UNKNOWN = 0        # Does not know about the authentication state
PROCESSING = 1     # Waiting for the oid server response about the auth
AUTHENTICATED = 2  # User authenticated correctly

DEFAULT_SESSION_NAME = 'SecurityModule'
DEFAULT_OID_SERVER = 'https://cmsweb.cern.ch/security/'

#-----------------------------------------------------------------------------
class OidConsumer(cherrypy.Tool):
    """
    A class to transparently transform a cherrypy-based web app into an OpenID
    consumer (DMWM OidApp), thus allowing it to do authentication/authorization
    with the CMS DMWM OpenID server.
    """
    
    def __init__(self, config):
        self.config = config
        self.decoder = json.JSONDecoder()

        # *** Fix: should support other different storage backends *** 
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
        #self.login_path = '%s/login' % self.base_path
        self.failed_path = '%s/failure' % self.base_path
        self.cancel_path = '%s/cancelled' % self.base_path
        self.error_path = '%s/error' % self.base_path
        self.logout_path = '%s/logout' % self.base_path
        self.authz_path = '%s/authz' % self.base_path
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
        #print '----A: Session data-----\n'+str(self.session())+'\n------------------------'
        # It is allways allowed access to 'auth' URLs like the page that shows
        # error information (pages under the config.mount_point)
        if cherrypy.request.script_name.startswith('/'+self.config.mount_point.rstrip('/')):
            return # skip pages to be handled by OidDefaultHandler.

        # The following is a bug fix. It is necessary because for the security
        # point of view, http://127.0.0.1/ is different from http://localhost/.
        # If we set a cookie (the session id) for http://localhost and then
        # after authenticating the server redirects us to http://127.0.0.1, the
        # browser will not use send cookie, then a new session will start and the
        # authentication will fail. The reason it fails is that some headers will
        # get duplicated and the openid server will respond with a form with a
        # single button to confirm. Then the consumer will not understand these
        # doubled headers and will fail.
        if not cherrypy.request.base.startswith(self.app_url):
            #redir_to = self.app_url+cherrypy.request.script_name+
            #cherrypy.request.path_info+cherrypy.request.path+'?'+urllib.urlencode(cherrypy.request.params)
            redir_to = cherrypy.url(qs=cherrypy.request.query_string, base=self.app_url)
            raise cherrypy.HTTPRedirect(redir_to)

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
        assert(self.session()['status']==AUTHENTICATED)
        #print '----B: Session data-----\n'+str(self.session())+'\n------------------------'
    
    def request_auth(self):
        """
        This method generates the OpenID request and redirects the user
        to the oid server to process it.
        """
        # Here it is where we start playing with OpenId
        oidconsumer = consumer.Consumer(self.session(), self.store)

        # In our case we don't need to discover because we know
        # the server we want to authenticate.
        oidrequest = oidconsumer.beginWithoutDiscovery(discover.OpenIDServiceEndpoint.fromOPEndpointURL(self.oidserver))

        # Then the authentication begins...
        self.session()['status'] = PROCESSING

        # Extends the OpenID request using SREG. Uses it to get authoriztion
        # data. Since this extension makes part of the original OpenID
        # request, it will be sent securely.
        sreg_request = sreg.SRegRequest(required=['permissions',
                                                      'fullname',
                                                      'dn'])
        oidrequest.addExtension(sreg_request)

        # Set the return URL to be the one requested by the user.
        return_to = cherrypy.url(qs=cherrypy.request.query_string, base=self.app_url)
        trust_root = self.app_url # was cherrypy.request.base

        # redirectURL() encodes the OpenID request into an URL
        redirect_url = oidrequest.redirectURL(trust_root, return_to)
        # Redirects the user-agent to the oid server using the
        # encoded URL. After authenticating the user, the oid server
        # will redirect the user agent back to 'return_to'
        raise cherrypy.HTTPRedirect(redirect_url)

        # End of request_auth()

    def verify_auth(self):
        """
        This method deals with the oid server response (when it redirects
        the user-agent back to here. This request contains the status of the
        user authentication with the oid server
        """
        sess=self.session()
        sess['status'] = UNKNOWN

        # Ask the oid library to verify the response received from the oid serv.
        current_url = cherrypy.url(qs=cherrypy.request.query_string, base=self.app_url)
        oidconsumer = consumer.Consumer(sess, self.store)
        oidresp = oidconsumer.complete(cherrypy.request.params, current_url)

        # Now verifies what the oid server response means
        if oidresp.status == consumer.SUCCESS:
            if oidresp.endpoint.canonicalID:
                # Should use canonicalIDs instead of user-friendly oid URLs,
                # but leave it like this now
                pass

            # Gets additional information that came in the server response.
            # The authorization information is supposed to come in here.
            sreg_data = sreg.SRegResponse.fromSuccessResponse(oidresp) or {}
            for i in ['fullname', 'dn']:
                sess[i] = sreg_data.get(i,None)
            # Should do a better job when passing a dict as a string
            sess['permissions'] = eval(sreg_data.get('permissions',"{}"))
            
            # Set the new session state to authenticated and saves the username
            sess['user_url'] = oidresp.getDisplayIdentifier()
            sess['status'] = AUTHENTICATED

            # Finally redirects the user-agent to the URL initially requested.
            # The last query string argument, the 'janrain_nonce', is removed
            # because app handler is not aware of it and thus may cause errors
            return_to = oidresp.getReturnTo().rsplit('janrain_nonce',1)[0]
            raise cherrypy.HTTPRedirect(return_to.rstrip('?&'))

        elif oidresp.status == consumer.FAILURE:
            # The OpenID protocol failed, either locally or remotely
            msg = ''
            if oidresp.identity_url:
                msg += '<br><br><b>Could not authenticate %s</b>' % oidresp.identity_url
            if oidresp.message:
                msg += '<br><br><b>%s</b>' % oidresp.message
            else:
                msg += '<br><br><b>Authentication failed for an unknown reason</b>'
            sess['debug_info'] = msg
            raise cherrypy.HTTPRedirect(self.failed_path)
        
        elif oidresp.status == consumer.CANCEL:
            # Indicates that the user cancelled the OpenID authentication request
            if oidresp.identity_url:
                msg = '<br><br><b>The authentication for %s' % oidresp.identity_url
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
            sess['debug_info'] = msg
            raise cherrypy.HTTPRedirect(self.cancel_path)

        elif oidresp.status == consumer.SETUP_NEEDED:
            # Means that the request was in immediate mode and the server was
            # unable to authenticate the user without interaction.
            # So, should send the user to the server
            if oidresp.setup_url: # maybe not available in OpenID 2.0
                raise cherrypy.HTTPRedirect(oidresp.setup_url)
            msg = '<br><br><b>Could not authenticate without user interaction.</b>'
            sess['debug_info'] = msg
            raise cherrypy.HTTPRedirect(self.failed_path)

        # If the code reaches here, an unknown error happened
        msg = '<br><br><b>Invalid response from the server.</b>'
        sess['debug_info'] = msg
        raise cherrypy.HTTPRedirect(self.error_path)

        # End of verify_auth()

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
            self.session()['debug_info'] = msg
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
            cherrypy.session[self.sessname]['status'] = UNKNOWN  # auth state of this session
            cherrypy.session[self.sessname]['user_url'] = None
                                  # The user related to this session
                                  # user_url = self.oidserver+'id/'+the real
                                  #            username seen by the oid server
                                  #            (will come from Hypernews/SiteDB)
            cherrypy.session[self.sessname]['debug_info'] = None
            cherrypy.session[self.sessname]['fullname'] = None
            cherrypy.session[self.sessname]['dn'] = None
            cherrypy.session[self.sessname]['permissions'] = None # user roles

        return cherrypy.session[self.sessname]

#-------------------------------------------------------------------------------
