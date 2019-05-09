import os, hmac, hashlib, cherrypy
from tempfile import NamedTemporaryFile
from WMCore.REST.Main import RESTMain
from WMCore.REST.Auth import authz_canonical
from WMCore.Configuration import Configuration

def fake_authz_headers(hmac_key, method = 'HNLogin',
                       login='testuser', name='Test User',
                       dn="/test/dn", roles={}, format="list"):
    """Create fake authentication and authorisation headers compatible
    with the CMSWEB front-ends. Assumes you have the HMAC signing key
    the back-end will use to validate the headers.

    :arg str hmac_key: binary key data for signing headers.
    :arg str method: authentication method, one of X509Cert, X509Proxy,
      HNLogin, HostIP, AUCookie or None.
    :arg str login: account login name.
    :arg str name: account user name.
    :arg str dn: account X509 subject.
    :arg dict roles: role dictionary, each role with 'site' and 'group' lists.
    :returns: list of header name, value tuples to add to a HTTP request."""
    headers = { 'cms-auth-status': 'OK', 'cms-authn-method': method }

    if login:
        headers['cms-authn-login'] = login

    if name:
        headers['cms-authn-name'] = name

    if dn:
        headers['cms-authn-dn'] = dn

    for name, role in roles.items():
        name = 'cms-authz-' + authz_canonical(name)
        headers[name] = []
        for r in 'site', 'group':
            if r in role:
                headers[name].extend(["%s:%s" % (r, authz_canonical(v)) for v in role[r]])
        headers[name] = " ".join(headers[name])

    prefix = suffix = ""
    hkeys = headers.keys()
    for hk in sorted(hkeys):
        if hk != 'cms-auth-status':
            prefix += "h%xv%x" % (len(hk), len(headers[hk]))
            suffix += "%s%s" % (hk, headers[hk])

    cksum = hmac.new(hmac_key, prefix + "#" + suffix, hashlib.sha1).hexdigest()
    headers['cms-authn-hmac'] = cksum
    if format == "list":
        return headers.items()
    else:
        return headers

def fake_authz_key_file(delete=True):
    """Create temporary file for fake authorisation hmac signing key.

    :returns: Instance of :class:`~.NamedTemporaryFile`, whose *data*
      attribute contains the HMAC signing binary key."""
    t = NamedTemporaryFile(delete=delete)
    with open("/dev/urandom") as fd:
        t.data = fd.read(20)
    t.write(t.data)
    t.seek(0)
    return t

def setup_dummy_server(module_name, class_name, app_name = None, authz_key_file=None, port=8888):
    """Helper function to set up a :class:`~.RESTMain` server from given
    module and class. Creates a fake server configuration and instantiates
    the server application from it.

    :arg str module_name: module from which to import test class.
    :arg str class_type: name of the server test class.
    :arg str app_name: optional test application name, 'test' by default.
    :returns: tuple with the server object and authz hmac signing key."""
    if authz_key_file:
        test_authz_key = authz_key_file
    else:
        test_authz_key = fake_authz_key_file()

    cfg = Configuration()
    main = cfg.section_('main')
    main.application = app_name or 'test'
    main.silent = True
    main.index = 'top'
    main.authz_defaults = { 'role': None, 'group': None, 'site': None }
    main.section_('tools').section_('cms_auth').key_file = test_authz_key.name

    app = cfg.section_(app_name or 'test')
    app.admin = 'dada@example.org'
    app.description = app.title = 'Test'

    views = cfg.section_('views')
    top = views.section_('top')
    top.object = module_name + "." + class_name

    server = RESTMain(cfg, os.getcwd())
    server.validate_config()
    server.setup_server()
    server.install_application()
    cherrypy.config.update({'server.socket_port': port})
    cherrypy.config.update({'server.socket_host': '127.0.0.1'})
    cherrypy.config.update({'request.show_tracebacks': True})
    cherrypy.config.update({'environment': 'test_suite'})
    for app in cherrypy.tree.apps.values():
        if '/' in app.config:
            app.config["/"]["request.show_tracebacks"] = True

    return server, test_authz_key
