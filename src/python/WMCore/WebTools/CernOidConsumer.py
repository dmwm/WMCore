#!/usr/bin/env python

import cherrypy
import openid
from openid.consumer import consumer, discover
from openid.cryptutil import randomString # To generate session IDs
from openid.store import filestore
import cgi # To use escape()
# default handler for login/etc pages
from WMCore.WebTools.CernOidDefaultHandler import CernOidDefaultHandler

# Define session states
UNKNOWN = 0        # Does not know about the authentication state
PROCESSING = 1     # Waiting for the oid server response about the auth
AUTHENTICATED = 2  # User authenticated correctly

DEFAULT_SESSION_NAME = 'CernOpenIdTool'
#-----------------------------------------------------------------------------
# A class to transparently transform a cherrypy-based web app into an OpenID
# consumer, thus allowing it to use CERN auth/authorization facilities
class CernOidConsumer(cherrypy.Tool):
    def __init__(self, config):
        self.config = config
        if self.config.store == 'filestore':
            self.store = filestore.FileOpenIDStore(self.config.store_path)
        
        self.session_name = self.config.session_name
        self.base_path = '/' # base_path + '/'
        self.login_path = '%s/login' % self.base_path
        self.failed_path = '%s/failure' % self.base_path
        self.cancel_path = '%s/cancelled' % self.base_path
        self.error_path = '%s/error' % self.base_path
        self.logout_path = '%s/logout' % self.base_path
        self.defhandler = CernOidDefaultHandler(self.session_name)

        # Defines the hook point for cherrypy
        self._name = None
        self._point = 'before_request_body'
        self._priority = 60 # Just after the sessions being enabled

    # This is the method that is called in the cherrypy hook point (whenever
    # the user-agent requests a page
    def callable(self):
        self.verify()
        self.process()
        self.defaults()
    
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
        # Do not verify auth URLs like the page that requests the user login
        if cherrypy.request.path_info.startswith(self.base_path):
            return

        # If the user requested to login again without loging out first,
        # force a logout
        openid_url = cherrypy.request.params.get('openid_url',None)
        if self.is_relogin(openid_url):
            del cherrypy.session[self.session_name] # logout before continuing
                
        # Do not start the verification process if it is was already started
        if self.is_processing():
            return

        # If the user didn't inform his ID yet, redirect him to the login page
        if not openid_url:
            raise cherrypy.HTTPRedirect(self.login_path)

        del cherrypy.request.params['openid_url']

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
            cherrypy.session[self.session_name]['return_to'] = return_to
            cherrypy.session[self.session_name]['status'] = PROCESSING

            # redirectURL() encodes the OpenID request into an URL
            redirect_url = oidrequest.redirectURL(cherrypy.request.base, return_to)
            # Redirects the user-agent to the oid server using the encoded URL
            # After authenticating the user, the oid server will redirect the user
            # agent back to 'return_to'
            raise cherrypy.HTTPRedirect(redirect_url)
        # End of verify()

    # This function deals with the oid server response (when it redirects
    # the user-agent back to here. This request contains the status of the
    # user authentication in the oid server
    def process(self):
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
            print "AUTH: %s" % info.message
            raise cherrypy.HTTPRedirect(self.failed_path)
        elif info.status == consumer.CANCEL:
            # Indicates that the user cancelled the OpenID authentication request
            print "AUTH: %s" % info.identity_url
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

            # Set the new session state to authenticated
            cherrypy.session[self.session_name]['status'] = AUTHENTICATED
            cherrypy.session[self.session_name]['openid_url'] = info.getDisplayIdentifier()
            cherrypy.request.params = {} # empty request parameters

            # Finally redirects the user-agent to the URL initially requested
            raise cherrypy.HTTPRedirect(cherrypy.url(cherrypy.request.path_info))

        # If the code reaches here, an unknown error happened
        cherrypy.session[self.session_name]['info'] = 'unknown reason'
        raise cherrypy.HTTPRedirect(self.error_path)

        # End of process()

    def defaults(self):
        if not cherrypy.request.path_info.startswith(self.base_path):
            return # We only need to worry about handlers for auth path

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