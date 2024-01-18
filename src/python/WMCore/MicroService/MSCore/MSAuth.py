"""
File       : MSAuth.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSAuth class provides auth/authz functionality for micro-services

The auth/authz schema is defined in JSON data-format as list of records, e.g.
[ record1, record2, ...] where individual record has the following structure
{"role": string, "group": string, "service": string, "action": string, "method": [string, string]}
where
- role defines authorization role, e.g. admin
- group defines specific group, e.g. reqmgr
- service identifies MS service, e.g. ms-pileup
- actions refers to a specific API or action, e.g. create
- method contains list of allowed HTTP methods, e.g. ["POST", "PUT"]

For example, here is a possible list of authz rules for MSPileup service

authz_rules = \
    [{"role": "admin", "group": "reqmgr", "service": "ms-pileup", "action": "create", "method": ["POST"]},
     {"role": "production-operator", "group": "dataops", "service": "ms-pileup", "action": "create", "method": ["POST"]},

     {"role": "admin", "group": "reqmgr", "service": "ms-pileup", "action": "delete", "method": ["DELETE"]},
     {"role": "production-operator", "group": "dataops", "service": "ms-pileup", "action": "delete", "method": ["DELETE"]},

     {"role": "admin", "group": "reqmgr", "service": "ms-pileup", "action": "update", "method": ["PUT"]},
     {"role": "production-operator", "group": "dataops", "service": "ms-pileup", "action": "update", "method": ["PUT"]}]
"""

# system modules
import os
import json
from pathlib import Path

# third party modules
import cherrypy

# WMCore modules
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.REST.Auth import user_info_from_headers
from Utils.Utilities import lowerCmsHeaders


def readAuthzRules(entry):
    """
    Return authz rules from given entry
    :param entry: either file name or list of authz rules, see docstring of this module
    :return: list of authz rules
    """
    rules = []
    if entry is None:
        return rules
    # of provided is a list data-type it may contaisn our rules
    if isinstance(entry, list) and len(entry) > 0:
        if isinstance(entry[0], dict):
            return entry
    # check if entry is a file name
    if os.path.exists(entry):
        with open(entry, 'r', encoding="utf-8") as fstream:
            rules = json.load(fstream)
            return rules
    msg = f"Unable to read {entry} as authz rules"
    raise Exception(msg)


class MSAuth():
    """
    This class provides auth/authz functionality for micro-services
    """

    def __init__(self, msConfig, **kwargs):
        """
        Provides a basic setup for all the microservices

        :param msConfig: MS service configuration
        :param kwargs: optional parameters
        """
        self.logger = getMSLogger(getattr(msConfig, 'verbose', False), kwargs.get("logger"))
        self.msConfig = msConfig
        self.logger.info("Configuration including default values:\n%s", self.msConfig)

        # load auth/authz configuration
        self.authzRules = readAuthzRules(msConfig.get('authz_rules', None))
        self.authzKey = msConfig['authz_key']  # hmac key of front-end
        # check if provided authz_key is a file name
        if isinstance(self.authzKey, str):
            authFile = Path(self.authzKey)
            if authFile.is_file():
                # msConfig provided file name with authz_key
                with open(authFile, 'rb') as istream:
                    self.authzKey = istream.read()

    def userInfo(self):
        """
        Read incoming HTTP request headers and return user info
        :return: user dict structure with HTTP headers key:value pairs, like
        {'dn': string, 'method': string, 'login': string, 'name': string, 'roles': {...}}
        """
        # skip authorization on localhost or if no remote address is provided
        # this is only valid if no cms auth headers is set, i.e. in unit tests
        headers = lowerCmsHeaders(cherrypy.request.headers)
        if 'cms-auth-status' not in headers:
            remoteAddr = headers.get('Remote-Addr', '')
            if remoteAddr in ['127.0.0.1', '']:
                user = {'dn': 'localhost-user', 'method': None, 'login': None, 'name': None, 'roles': {}}
                return user

        # get user information
        user = user_info_from_headers(key=self.authzKey)
        return user

    def authorizeApiAccess(self, service, action, method=None):
        """
        Check auth role based CMS HTTP AUTH headers: cms-auth-status,
        cms-authn, cms-authz, cms-authn-hmac, cms-authn-name, cms-authn-dn.
        It throws cherrypy HTTPError if user is not authorized

        :param service: service name (string)
        :param action: action name (string
        :param method: method name (string)
        :return: none
        """
        # get user information
        user = self.userInfo()
        if user.get('dn', '') == 'localhost-user':
            # we allow localhost-user to perform any actions (unit test scenario)
            # see userInfo method which generates localhost-user DN. This fixes
            # (403, 'You are not authorized to access this resource.') error in unit tests
            return

        # CMS AUTH headers
        # cms-auth-status, cms-authn, cms-authz, cms-authn-hmac, cms-authn-name, cms-authn-dn
        for entry in self.authzRules:
            # check that user method is allowed
            if method and method not in entry['method']:
                continue
            userMethod = user.get('method', None)
            if userMethod and userMethod != "X509Cert" and userMethod not in entry['method']:
                continue

            # check if entry service and action matches
            if entry['service'] == service and entry['action'] == action:
                # if we have match for service and action we'll check used roles
                # 'roles': {'fake': {'site': set(['fake']), 'group': set(['fake'])}}
                for role, obj in user['roles'].items():
                    if role != entry['role']:
                        continue
                    for group in obj['group']:
                        if group == entry['group']:
                            return

        # if we reach this place we did not match anything and should through exception
        self.logger.error("ERROR: not authorized access to %s method %s", service, action)
        raise cherrypy.HTTPError(403, "You are not authorized to access this resource.")
