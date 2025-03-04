from builtins import str, zip, range, object
from future.utils import viewitems, viewvalues, listvalues

import cherrypy
import inspect
import os
import re
import signal
from string import ascii_letters as letters
import time
from collections import namedtuple
from functools import wraps
from threading import Thread, Condition

from cherrypy import engine, expose, request, response, HTTPError, HTTPRedirect, tools
from cherrypy.lib import cpstats

from WMCore.REST.Error import *
from WMCore.REST.Format import *
from WMCore.REST.Validation import validate_no_more_input
from Utils.CPMetrics import promMetrics

from Utils.Utilities import encodeUnicodeToBytes

try:
    from cherrypy.lib import httputil
except:
    from cherrypy.lib import http as httputil

#: List of HTTP methods for which it's possible to register a REST handler.
_METHODS = ('GET', 'HEAD', 'POST', 'PUT', 'DELETE')

#: Regexp for censoring passwords out of SQL statements before logging them.
_RX_CENSOR = re.compile(r"(identified by) \S+", re.I)

#: MIME types which are compressible.
_COMPRESSIBLE = ['text/html', 'text/html; charset=utf-8',
                 'text/plain', 'text/plain; charset=utf-8',
                 'text/css', 'text/css; charset=utf-8',
                 'text/javascript', 'text/javascript; charset=utf-8',
                 'application/json']

#: Type alias for arguments passed to REST validation methods, consisting
#: of `args`, the additional path arguments, and `kwargs`, the query
#: arguments either from the query string or body (but not both).
RESTArgs = namedtuple("RESTArgs", ["args", "kwargs"])


######################################################################
######################################################################
class RESTFrontPage(object):
    """Base class for a trivial front page intended to hand everything
    over to a javascript-based user interface implementation.

    This front-page simply serves static content like HTML pages, CSS,
    JavaScript and images from number of configurable document roots.
    Text content such as CSS and JavaScript can be scrunched together,
    or combo-loaded, from several files. All the content supports the
    standard ETag and last-modified validation for optimal caching.

    The base class assumes the actual application consists of a single
    front-page, which loads JavaScript and other content dynamically,
    and uses HTML5 URL history features to figure out what to do. That
    is, given application mount point <https://cmsweb.cern.ch/app>, all
    links such as <https://cmsweb.cern.ch/app/foo/bar?q=xyz> get mapped
    to the same page, which then figures out what to do at /foo/bar
    relative location or with the query string part.

    There is a special response for ``rest/preamble.js`` static file.
    This will automatically generate a scriptlet of the following form,
    plus any additional content passed in `preamble`::

      var REST_DEBUG = (debug_mode),
          REST_SERVER_ROOT = "(mount)",
          REST_INSTANCES = [{ "id": "...", "title": "...", "rank": N }...];

    REST_DEBUG, ``debug_mode``
      Is set to true/false depending on the value of the constructor
      :ref:`debug_mode` parameter, or if the default None, it's set
      to false if running with minimised assets, i.e. frontpage matches
      ``*-min.html``, true otherwise.

    REST_SERVER_ROOT, ``mount``
      The URL mount point of this object, needed for history init. Taken
      from the constructor argument.

    REST_INSTANCES
      If the constructor is given `instances`, its return value is turned
      into a sorted JSON list of available instances for JavaScript. Each
      database instance should have the dictionary keys "``.title``" and
      "``.order``" which will be used for human visible instance label and
      order of appearance, respectively. The ``id`` is the label to use
      for REST API URL construction: the instance dictionary key. This
      variable will not be emitted at all if :ref:`instances` is None.

    .. rubric:: Attributes

    .. attribute:: _app

       Reference to the application object given to the constructor.

    .. attribute:: _mount

       The mount point given in the constructor.

    .. attribute:: _static

       The roots given to the constructor, with ``rest/preamble.js`` added.

    .. attribute:: _frontpage

       The name of the front page file.

    .. attribute:: _substitutions

       Extra (name, value) substitutions for `_frontpage`. When serving
       the front-page via `_serve()`, each ``@name@`` is replaced by its
       corresponding `value`.

    .. attribute:: _embeddings

       Extra (name, value) file replacements for `_frontpage`. Similar to
       `_substitutions` but each value is a list of file names, and the
       replacement value is the concatenation of all the contents of all
       the files, with leading and trailing white space removed.

    .. attribute:: _preamble

       The ``rest/preamble.js`` computed as described above.

    .. attribute:: _time

       Server start-up time, used as mtime for ``rest/preamble.js``.
    """

    def __init__(self, app, config, mount, frontpage, roots,
                 substitutions=None, embeddings=None,
                 instances=None, preamble=None, debug_mode=None):
        """.. rubric:: Constructor

        :arg app:                Reference to the :class:`~.RESTMain` application.
        :arg config:             :class:`~.WMCore.ConfigSection` for me.
        :arg str mount:          URL tree mount point for this object.
        :arg str frontpage:      Name of the front-page file, which must exist in
                                 one of the `roots`. If `debug_mode` is None and
                                 the name matches ``*-min.html``, then debug mode
                                 is set to False, True otherwise.
        :arg dict roots:         Dictionary of roots for serving static files.
                                 Each key defines the label and path root for URLs,
                                 and the value should have keys "``root``" for the
                                 path to start looking up files, and "``rx``" for
                                 the regular expression to define valid file names.
                                 **All the root paths must end in a trailing slash.**
        :arg dict substitutions: Extra (name, value) substitutions for `frontpage`.
        :arg dict embeddings:    Extra (name, value) file replacements for `frontpage`.
        :arg callable instances: Callable which returns database instances, often
                                 ``lambda: return self._app.views["data"]._db``
        :arg str preamble:       Optional string for additional content for the
                                 pseudo-file ``rest/preamble.js``.
        :arg bool debug_mode:    Specifies how to set REST_DEBUG, see above."""

        # Verify all roots do end in a slash.
        for origin, info in viewitems(roots):
            if not re.match(r"^[-a-z0-9]+$", origin):
                raise ValueError("invalid root label")
            if not info["root"].endswith("/"):
                raise ValueError("%s 'root' must end in a slash" % origin)

        # Add preamble pseudo-root.
        roots["rest"] = {"root": None, "rx": re.compile(r"^preamble(?:-min)?\.js$")}

        # Save various things.
        self._start = time.time()
        self._app = app
        self._mount = mount
        self._frontpage = frontpage
        self._static = roots
        self._substitutions = substitutions or {}
        self._embeddings = embeddings or {}
        if debug_mode is None:
            debug_mode = not frontpage.endswith("-min.html")

        # Delay preamble setup until server start-up so that we don't try to
        # dereference instances() until after it's been finished constructing.
        engine.subscribe("start", lambda: self._init(debug_mode, instances, preamble), 0)

    def _init(self, debug_mode, instances, preamble):
        """Delayed preamble initialisation after server is fully configured."""
        self._preamble = ("var REST_DEBUG = %s" % ((debug_mode and "true") or "false"))
        self._preamble += (", REST_SERVER_ROOT = '%s'" % self._mount)

        if instances:
            instances = [dict(id=k, title=v[".title"], order=v[".order"])
                         for k, v in viewitems(instances())]
            instances.sort(key=lambda x: x["order"])
            self._preamble += (", REST_INSTANCES = %s" % json.dumps(instances))

        self._preamble += ";\n%s" % (preamble or "")

    def _serve(self, items):
        """Serve static assets.

        Serve one or more files. If there is just one file, it can be text or
        an image. If there are several files, they are smashed together as a
        combo load operation. In that case it's assumed the files are compatible,
        for example all JavaScript or all CSS.

        All normal response headers are set correctly, including Content-Type,
        Last-Modified, Cache-Control and ETag. Automatically handles caching
        related request headers If-Match, If-None-Match, If-Modified-Since,
        If-Unmodified-Since and responds appropriately. The caller should use
        CherryPy gzip tool to handle compression-related headers appropriately.

        In general files are passed through unmodified. The only exception is
        that HTML files will have @MOUNT@ string replaced with the mount point,
        the @NAME@ substitutions from the constructor are replaced by value,
        and @NAME@ embeddings are replaced by file contents.

        :arg list(str) items: One or more file names to serve.
        :returns: File contents combined as a single string."""
        mtime = 0
        result = ""
        ctype = ""

        if not items:
            raise HTTPError(404, "No such file")

        for item in items:
            # There must be at least one slash in the file name.
            if item.find("/") < 0:
                cherrypy.log("ERROR: directory required for front-page '%s'" % item)
                raise HTTPError(404, "No such file")

            # Split the name to the origin part - the name we look up in roots,
            # and the remaining path part for the rest of the name under that
            # root. For example 'yui/yui/yui-min.js' means we'll look up the
            # path 'yui/yui-min.js' under the 'yui' root.
            origin, path = item.split("/", 1)
            if origin not in self._static:
                cherrypy.log("ERROR: front-page '%s' origin '%s' not in any static root"
                             % (item, origin))
                raise HTTPError(404, "No such file")

            # Look up the description and match path name against validation rx.
            desc = self._static[origin]
            if not desc["rx"].match(path):
                cherrypy.log("ERROR: front-page '%s' not matched by rx '%s' for '%s'"
                             % (item, desc["rx"].pattern, origin))
                raise HTTPError(404, "No such file")

            # If this is not the pseudo-preamble, make sure the requested file
            # exists, and if it does, read it and remember its mtime. For the
            # pseudo preamble use the precomputed string and server start time.
            if origin != "rest":
                fpath = desc["root"] + path
                if not os.access(fpath, os.R_OK):
                    cherrypy.log("ERROR: front-page '%s' file does not exist" % item)
                    raise HTTPError(404, "No such file")
                try:
                    mtime = max(mtime, os.stat(fpath).st_mtime)
                    data = open(fpath).read()
                except:
                    cherrypy.log("ERROR: front-page '%s' failed to retrieve file" % item)
                    raise HTTPError(404, "No such file")
            elif self._preamble:
                mtime = max(mtime, self._start)
                data = self._preamble
            else:
                cherrypy.log("ERROR: front-page '%s' no preamble for 'rest'?" % item)
                raise HTTPError(404, "No such file")

            # Concatenate contents and set content type based on name suffix.
            ctypemap = {"js": "text/javascript; charset=utf-8",
                        "css": "text/css; charset=utf-8",
                        "html": "text/html; charset=utf-8"}
            suffix = path.rsplit(".", 1)[-1]
            if suffix in ctypemap:
                if not ctype:
                    ctype = ctypemap[suffix]
                elif ctype != ctypemap[suffix]:
                    ctype = "text/plain"
                if suffix == "html":
                    for var, value in viewitems(self._substitutions):
                        data = data.replace("@" + var + "@", value)
                    for var, files in viewitems(self._embeddings):
                        value = ""
                        for fpath in files:
                            if not os.access(fpath, os.R_OK):
                                cherrypy.log("ERROR: embedded '%s' file '%s' does not exist"
                                             % (var, fpath))
                                raise HTTPError(404, "No such file")
                            try:
                                mtime = max(mtime, os.stat(fpath).st_mtime)
                                value += open(fpath).read().strip()
                            except:
                                cherrypy.log("ERROR: embedded '%s' file '%s' failed to"
                                             " retrieve file" % (var, fpath))
                                raise HTTPError(404, "No such file")
                        data = data.replace("@" + var + "@", value)
                    data = data.replace("@MOUNT@", self._mount)
                if result:
                    result += "\n"
                result += data
                if not result.endswith("\n"):
                    result += "\n"
            elif suffix == "gif":
                ctype = "image/gif"
                result = data
            elif suffix == "png":
                ctype = "image/png"
                result = data
            else:
                raise HTTPError(404, "Unexpected file type")

        # Build final response + headers.
        response.headers['Content-Type'] = ctype
        response.headers['Last-Modified'] = httputil.HTTPDate(mtime)
        response.headers['Cache-Control'] = "public, max-age=%d" % 86400
        response.headers['ETag'] = '"%s"' % hashlib.sha1(encodeUnicodeToBytes(result)).hexdigest()
        cherrypy.lib.cptools.validate_since()
        cherrypy.lib.cptools.validate_etags()
        return result

    @expose
    @tools.gzip(compress_level=9, mime_types=_COMPRESSIBLE)
    def static(self, *args, **kwargs):
        """Serve static assets.

        Assumes a query string in the format used by YUI combo loader, with one
        or more file names separated by ampersands (&). Each name must be a plain
        file name, to be found in one of the roots given to the constructor.

        Path arguments must be empty, or consist of a single 'yui' string, for
        use as the YUI combo loader. In that case all file names are prefixed
        with 'yui/' to make them compatible with the standard combo loading.

        Serves assets as documented in :meth:`_serve`."""
        # The code was originally designed to server YUI content.
        # Modified to support any content by joining args into single path
        # on web page it can be /path/static/js/file.js or /path/static/css/file.css
        if len(args) > 1 or (args and args[0] != "yui"):
            return self._serve(['/'.join(args)])
        # Path arguments must be empty, or consist of a single 'yui' string,
        paths = request.query_string.split("&")
        if not paths:
            raise HTTPError(404, "No such file")
        if args:
            paths = [args[0] + "/" + p for p in paths]
        return self._serve(paths)

    @expose
    def feedback(self, *args, **kwargs):
        """Receive browser problem feedback. Doesn't actually do anything, just
        returns an empty string response."""
        return ""

    @expose
    @tools.gzip(compress_level=9, mime_types=_COMPRESSIBLE)
    def default(self, *args, **kwargs):
        """Generate the front page, as documented in :meth:`_serve`. The
        JavaScript will actually work out what to do with the rest of the
        URL arguments; they are not used here."""
        return self._serve([self._frontpage])

    @expose
    def stats(self):
        """
        Return CherryPy stats dict about underlying service activities
        """
        return cpstats.StatsPage().data()

    @expose
    def metrics(self):
        """
        Return CherryPy stats following the prometheus metrics structure
        """
        metrics = promMetrics(cpstats.StatsPage().data(), self.app.appname)
        return encodeUnicodeToBytes(metrics)


######################################################################
######################################################################
class MiniRESTApi(object):
    """Minimal base class for REST services.

    .. rubric:: Overview

    This is the main base class for the CherryPy-based REST API interfaces.
    It provides a minimal interface for registering API methods to be called
    via REST HTTP calls. Normally implementations should derive from the
    :class:`~.RESTApi` higher level abstraction.

    Instances of this class maintain a table of handlers associated to HTTP
    method (GET, HEAD, POST, PUT, or DELETE) and an API word. The given API
    word may support any number of different HTTP methods by associating
    them to the different, or even same, callables. Each such API may be
    associated with arbitrary additional arguments, typically options to
    adapt behaviour for the API as described below.

    Each API handler must be associated to a validation method which will
    be given the remaining URL path and query arguments. The validator
    must *verify* the incoming arguments and *move* safe input to output
    arguments, typically using :mod:`~.RESTValidation` utilities. If any
    arguments remain in input after validation, the call is refused. This
    implies the only way to get *any input at all* to the REST API is to
    introduce a validator code, which must actively process every piece
    of input to be used later in the actual method.

    When invoked by CherryPy, the class looks up the applicable handler by
    HTTP request method and the first URL path argument, runs validation,
    invokes the method, and formats the output, which should be some python
    data structure which can be rendered into JSON or XML, or some custom
    format (e.g. raw bytes for an image). For sequences and sequence-like
    results, the output is streamed out, object by object, as soon as the
    handler returns; the entire response is never built up in memory except
    as described below for compression and entity tag generation. As the
    assumption is that method will generate some pythonic data structure,
    the caller is required to specify via "Accept" header the desired
    output format.

    .. rubric:: Validation

    There is some general validation before invoking the API method. The
    HTTP method is first checked against allowed methods listed above, and
    those for which there is a registered API handler. The API is checked
    to be known, and that a handler has been registered for the combination
    of the two, and that method can produce output in the format requested
    by in the "Accept" header. The derived class can run :meth:`_precall`
    validation hook before the API name is popped off URL path arguments,
    in case it wants to pull other arguments such as database instance
    label from the URL first.

    The API-specific validation function is called with argument list
    *(apiobj, method, apiname, args, safeargs).* The *apiobj* is the metadata
    object for the API entry created by :meth:`_addAPI`, *method* the HTTP
    method, *apiname* the API name. The remaining two arguments are incoming
    and validated safe arguments, as described above for validation; both
    are instances of :class:`RESTArgs`.

    .. rubric:: Method call

    The API handler is invoked with arguments and keyword arguments saved
    into *safeargs* by the validation method. The method returns a value,
    which must be either a python string, a sequence or a generator.

    .. rubric:: Output formatting

    Once the REST API has returned a value, the class formats the output.
    For GET and HEAD requests, expire headers are set as specified by the
    API object. After that the format handler is invoked, whose output is
    compressed if allowed by the API and the HTTP request, and entity tag
    headers are processed and generated. Although the API may return data
    in any form for which there is a formatter (cf. :class:`~.JSONFormat`,
    :class:`~.XMLFormat` and :class:`~.RawFormat`) that can be matched to
    the request "Accept" header, the general assumption is APIs return
    "rows" of "objects" which are output with "Transfer-Encoding: chunked"
    in streaming format.

    It is supported and recommended that APIs verify the operation will
    very likely succeed, then return a python *generator* for the result,
    for example a database cursor for query results. This allows large
    responses to be streamed out as they are produced, without ever
    buffering the entire response as a string in memory.

    The output from the response formatter is sent to a compressor if the
    request headers include suitable "Accept-Encoding" header and the API
    object hasn't declined compression. Normally all output is compressed
    with ZLIB level 9 provided the client supports ``deflate`` encoding.
    It is recommended to keep compression enabled except if the output is
    known to be incompressible, e.g. images or compressed data files. The
    added CPU use is usually well worth the network communication savings.

    .. rubric:: Entity tags and response caching

    Before discussing caching, we note the following makes the implicit
    assumption that GET and HEAD requests are idempotent and repeatable
    withut ill effects. It is expected only PUT, POST and DELETE have
    side effects.

    All GET and HEAD responses will get an entity tag, or ETag, header. If
    the API does not generate an ETag, a hash digest is computed over the
    serialised response as it's written out, and included in the trailer
    headers of the response.

    If the API can compute an ETag in a better way than hashing the output,
    or can reply to If-Match / If-None-Match queries without computing the
    result, it should absolutely do so. Since most servers generate dynamic
    content such as database query results, it's hard to generate a useful
    ETag, and the scheme used here, a digest over the content, allows many
    clients like browsers to cache the responses. The server still has to
    re-execute the query and reformat the result to recompute the ETag, but
    avoids having to send matching responses back, and more importantly, the
    client learns its cached copy remains valid and can optimise accordingly.

    As a usability and performance optimisation, small GET/HEAD responses up
    to :attr:`etag_limit` bytes are buffered entirely, the ETag is computed,
    any If-Match, If-None-Match request headers are processed, the ETag is
    added to the response headers, and the entire body is output as a single
    string without the regular chunked streaming. This effectively allows
    web browser clients to cache small responses even if the API method is
    unable to compute a reliable ETag for it. This covers virtually all
    responses one would access in a browser for most servers in practice.

    The scheme also reduces network traffic since the body doesn't need to be
    output for successful cache validation queries, and when body is output,
    TCP tends to get fed more efficiently with larger output buffers. On the
    other hand the scheme degrades gracefully for large replies while allowing
    smarter clients, such as those using curl library to talk to the server,
    to get more out of the API, both performance and functionality wise.

    For large responses the computed digest ETag is still added as a trailer
    header, after all the output has been emitted. These will not be useful
    for almost any client. For one, many clients ignore trailers anyway. For
    another there is no way to perform If-Match / If-None-Match validation.

    The default digest algorithm is :class:`~.SHA1ETag`, configurable via
    :attr:`etagger`. The tagging and compression work also if the API uses
    CherryPy's ``serve_file()`` to produce the response.

    For GET responses to be cached by clients the response must include a
    both a "Cache-Control" header and an ETag (or "Last-Modified" time). If
    :attr:`default_expires` and :attr:`default_expires_opts` settings allow
    it, GET responses will include a "Cache-Control: max-age=n" header.
    These can be tuned per API with ``cherrypy.tools.expires(secs=n)``, or
    ``expires`` and ``expires_opts`` :func:`restcall` keyword arguments.

    .. rubric:: Notes

    .. note:: Only GET and HEAD requests are allowed to have a query string.
       Other methods (POST, PUT, DELETE) may only have parameters specified
       in the request body. This is protection against XSRF attacks and in
       general attempts to engineer someone to submit a malicious HTML form.

    .. note:: The ETag generated by the default hash digest is computed from
       the formatted stream, before compression. It is therefore independent
       of "Accept-Encoding" request headers. However the ETag value is stable
       only if the formatter produces stable output given the same input. In
       particular any output which includes python dictionaries may vary over
       calls because of changes in dictionary key iteration order.

    .. warning:: The response generator returned by the API may fail after it
       has started to generate output. If the response is large enough not
       to fit in the ETag buffer, there is really no convenient way to stop
       producing output and indicate an error to the client since the HTTP
       headers have already been sent out with a good chunk of normal output.

       If the API-returned generator throws, this implementation closes the
       generated output by adding any required JSON, XML trailers and sets
       X-REST-Status and normal error headers in the trailer headers. The
       clients that understand trailer headers, such as curl-based ones, can
       use the trailer to discover the output is incomplete. Clients which
       ignore trailers, such as web browsers and python's urllib and httplib,
       will not know the output is truncated. Hence derived classes should be
       conservative about using risky constructs in the generator phase.

       Specifically any method with side effects should return a generator
       response only *after* all side effects have already taken place. For
       example when updating a database, the client should commit first,
       then return a generator. Because of how generators work in python,
       the latter step must be done in a separate function from the one that
       invokes commit!

       None of this really matters for responses which fit into the ETag
       buffer after compression. That is all but the largest responses in
       most servers, and the above seems a reasonable trade-off between
       not having to buffer huge responses in memory and usability.

    .. rubric:: Attributes

    .. attribute:: app

       Reference to the :class:`~.RESTMain` object from constructor.

    .. attribute:: config

       Reference to the :class:`WMCore.Configuraation` section for this API
       mount point as passed in to the constructor by :class:`~.RESTMain`.

    .. attribute:: methods

       A dictionary of registered API methods. The keys are HTTP methods,
       the values are another dictionary level with API name as a key, and
       an API object as a value. Do not modify this table directly, use the
       :meth:`_addAPI` method instead.

    .. attribute:: formats

       Possible output formats matched against "Accept" headers. This is a
       list of *(mime-type, formatter)* tuples. This is the general list of
       formats supported by this API entry point, with the intention that
       all or at least most API methods support these formats. Individual
       API methods can override the format list using ``formats`` keyword
       argument to :func:`restcall`, for example to specify list of image
       formats for one method which retrieves image files
       (cf. :class:`~.RawFormat`).

    .. attribute:: etag_limit

       An integer threshold for number of bytes to buffer internally for
       calculation of ETag header. See the description above for the full
       details on the scheme. The API object can override this limit with
       the ``etag_limit`` keyword argument to :func:`restcall`. The default
       is 8 MB.

    .. attribute:: compression

       A list of accepted compression mechanisms to be matched against the
       "Accept-Encoding" HTTP request header. Currently supported values are
       ``deflate`` and ``identity``. Using ``identity`` or emptying the list
       disables compression. The default is ``['deflate']``. Change this only
       for API mount points which are known to generate incompressible output,
       using ``compression`` keyword argument to :func:`restcall`.

    .. attribute:: compression_level

       Integer 0-9, the default ZLIB compression level for ``deflate`` encoding.
       The default is the maximum 9; for most servers the increased CPU use is
       usually well worth the reduction in network transmission costs. Setting
       the level to zero disables compression. The API can override this value
       with ``compression_level`` keyword argument to :func:`restcall`.

    .. attribute:: compression_chunk

       Integer, the approximate amount of input, in bytes, to consume to form
       compressed output chunks. The preferred value is the ZLIB compression
       horizon, 64kB. Up to this many bytes of output from the stream formatter
       are fed to the compressor, after which the compressor is flushed and the
       compressed output is emitted as HTTP transmission level chunk. Hence the
       receiving side can process chunks much like if the data wasn't compressed:
       the only difference is each HTTP chunk will contain an integral number of
       chunks instead of just one. (Of course in most cases the ETag buffering
       scheme will consume the output and emit it as a single unchunked part.)
       The API can override this value with ``compression_chunk`` keyword
       argument to :func:`restcall`.

    .. attribute:: default_expires

       Number, default expire time for GET / HEAD responses in seconds. The
       API object can override this value with ``expires`` keyword argument
       to :func:`restcall`. The default is one hour, or 3600 seconds.

    .. attribute:: default_expires_opts

       A sequence of strings, additional default options for "Cache-Control"
       response headers for responses with non-zero expire time limit. The
       strings are joined comma-separated to the "max-age=n" item. The API
       object can override this value with ``expires_opts`` keyword argument
       to :func:`restcall`. The default is an empty list.

    .. rubric:: Constructor arguments

    :arg app: The main application :class:`~.RESTMain` object.
    :arg config: The :class:`~.WMCore.ConfigSection` for this object.
    :arg str mount: The CherryPy URL tree mount point for this object.
    """

    def __init__(self, app, config, mount):
        self.app = app
        self.config = config
        self.etag_limit = 8 * 1024 * 1024
        self.compression_level = 9
        self.compression_chunk = 64 * 1024
        self.compression = ['deflate', 'gzip']
        self.formats = [('application/json', JSONFormat()),
                        ('application/xml', XMLFormat(self.app.appname))]
        self.methods = {}
        self.default_expires = 3600
        self.default_expires_opts = []

    def _addAPI(self, method, api, callable, args, validation, **kwargs):
        """Add an API method.

        Use this method to register handlers for a method/api combination. This
        creates an internal "API object" which internally represents API target.
        The API object is dictionary and will be passed to validation functions.

        :arg str method: The HTTP method name: GET, HEAD, PUT, POST, or DELETE.
        :arg str api: The API label, to be matched with first URL path argument.
          The label may not contain slashes.
        :arg callable callable: The handler; see class documentation for signature.
        :arg list args: List of valid positional and keyword argument names.
          These will be copied to the API object which will be passed to the
          validation methods. Normally you'd get these with inspect.getfullargspec().
        :arg callable validation: The validator; see class documentation for the
          signature and behavioural requirements. If `args` is non-empty,
          `validation` is mandatory; if `args` is empty, `callable` does not
          receive any input. The validator must copy validated safe input to
          actual arguments to `callable`.
        :arg dict kwargs: Additional key-value pairs to set in the API object.

        :returns: Nothing."""

        if method not in _METHODS:
            raise UnsupportedMethod()

        if method not in self.methods:
            self.methods[method] = {}

        if api in self.methods[method]:
            raise ObjectAlreadyExists()

        if not isinstance(args, list):
            raise TypeError("args is required to be a list")

        if not isinstance(validation, list):
            raise TypeError("validation is required to be a list")

        if args and not validation:
            raise ValueError("non-empty validation required for api taking arguments")

        apiobj = {"args": args, "validation": validation, "call": callable}
        apiobj.update(**kwargs)
        self.methods[method][api] = apiobj

    @expose
    def stats(self):
        """
        Return CherryPy stats dict about underlying service activities
        """
        return cpstats.StatsPage().data()

    @expose
    def metrics(self):
        """
        Return CherryPy stats following the prometheus metrics structure
        """
        metrics = promMetrics(cpstats.StatsPage().data(), self.app.appname)
        return encodeUnicodeToBytes(metrics)

    @expose
    def default(self, *args, **kwargs):
        """The HTTP request handler.

        This just wraps `args` and `kwargs` into a :class:`RESTArgs` and invokes
        :meth:`_call` enclosed in a try/except which filters all exceptions but
        :class:`HTTPRedirect` via :func:`~.report_rest_error`.

        In other words the main function of this wrapper is to ensure run-time
        errors are properly logged and translated to meaningful response status
        and headers, including a trace identifier for this particular error. It
        also sets X-REST-Time response header to the total time spent within
        this request.

        This method is declared "``{response.stream: True}``" to CherryPy.

        :returns: See :meth:`_call`."""

        try:
            return self._call(RESTArgs(list(args), kwargs))
        except HTTPRedirect:
            raise
        except cherrypy.HTTPError:
            raise
        except Exception as e:
            report_rest_error(e, format_exc(), True)
        finally:
            if getattr(request, 'start_time', None):
                response.headers["X-REST-Time"] = "%.3f us" % \
                                                  (1e6 * (time.time() - request.start_time))

    default._cp_config = {'response.stream': True}

    def _call(self, param):
        """The real HTTP request handler.

        :arg RESTArgs param: Input path and query arguments.

        :returns: Normally a generator over the input, but in some cases a
          plain string, as described in the class-level documentation. The
          API handler response, often a generator itself, is usually wrapped
          in couple of layers of additional generators which format and
          compress the output, and handle entity tags generation and matching."""

        # Make sure the request method is something we actually support.
        if request.method not in self.methods:
            response.headers['Allow'] = " ".join(sorted(self.methods.keys()))
            raise UnsupportedMethod() from None

        # If this isn't a GET/HEAD request, prevent use of query string to
        # avoid cross-site request attacks and evil silent form submissions.
        # We'd prefer perl cgi behaviour where query string and body args remain
        # separate, but that's not how cherrypy works - it mixes everything
        # into one big happy kwargs.
        if (request.method != 'GET' and request.method != 'HEAD') \
                and request.query_string:
            response.headers['Allow'] = 'GET HEAD'
            raise MethodWithoutQueryString()

        # Give derived class a chance to look at arguments.
        self._precall(param)

        # Make sure caller identified the API to call and it is available for
        # the request method.
        if len(param.args) == 0:
            raise APINotSpecified()
        api = param.args.pop(0)
        if api not in self.methods[request.method]:
            methods = " ".join(sorted([m for m, d in viewitems(self.methods) if api in d]))
            response.headers['Allow'] = methods
            if not methods:
                msg = 'Api "%s" not found. This method supports these Apis: %s' % (api, list(self.methods[request.method]))
                raise APINotSupported(msg)
            else:
                msg = 'Api "%s" only supported in method(s): "%s"' % (api, methods)
                raise APIMethodMismatch(msg)
        apiobj = self.methods[request.method][api]

        # Check what format the caller requested. At least one is required; HTTP
        # spec says no "Accept" header means accept anything, but that is too
        # error prone for a REST data interface as that establishes a default we
        # cannot then change later. So require the client identifies a format.
        # Browsers will accept at least */*; so can clients who don't care.
        # Note that accept() will raise HTTPError(406) if no match is found.
        # Available formats are either specified by REST method, or self.formats.
        try:
            if not request.headers.elements('Accept'):
                raise NotAcceptable('Accept header required')
            formats = apiobj.get('formats', self.formats)
            format = cherrypy.lib.cptools.accept([f[0] for f in formats])
            fmthandler = [f[1] for f in formats if f[0] == format][0]
        except HTTPError:
            format_names = ', '.join(f[0] for f in formats)
            raise NotAcceptable('Available types: %s' % format_names)

        # Validate arguments. May convert arguments too, e.g. str->int.
        safe = RESTArgs([], {})
        for v in apiobj['validation']:
            v(apiobj, request.method, api, param, safe)
        validate_no_more_input(param)

        # Invoke the method.
        obj = apiobj['call'](*safe.args, **safe.kwargs)

        # Add Vary: Accept header.
        vary_by('Accept')

        # Set expires header if applicable. Note that POST/PUT/DELETE are not
        # cacheable to begin with according to HTTP/1.1 specification. We must
        # do this before actually streaming out the response below in case the
        # ETag matching decides the previous response remains valid.
        if request.method == 'GET' or request.method == 'HEAD':
            expires = self.default_expires
            cpcfg = getattr(apiobj['call'], '_cp_config', None)
            if cpcfg and 'tools.expires.on' in cpcfg:
                expires = cpcfg.get('tools.expires.secs', expires)
            expires = apiobj.get('expires', expires)
            if 'Cache-Control' in response.headers:
                pass
            elif expires < 0:
                response.headers['Pragma'] = 'no-cache'
                response.headers['Expires'] = 'Sun, 19 Nov 1978 05:00:00 GMT'
                response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate,' \
                                                    ' post-check=0, pre-check=0'
            elif expires != None:
                expires_opts = apiobj.get('expires_opts', self.default_expires_opts)
                expires_opts = (expires_opts and ', '.join([''] + expires_opts)) or ''
                response.headers['Cache-Control'] = 'max-age=%d%s' % (expires, expires_opts)

        # Format the response.
        response.headers['X-REST-Status'] = 100
        response.headers['Content-Type'] = format
        etagger = apiobj.get('etagger', None) or SHA1ETag()
        reply = stream_compress(fmthandler(obj, etagger),
                                apiobj.get('compression', self.compression),
                                apiobj.get('compression_level', self.compression_level),
                                apiobj.get('compression_chunk', self.compression_chunk))
        return stream_maybe_etag(apiobj.get('etag_limit', self.etag_limit), etagger, reply)

    def _precall(self, param):
        """Point for derived classes to hook into prior to peeking at URL.

        Derived classes receive the incoming path and keyword arguments in
        `param`, and can peek or modify those as appropriate. In particular
        if they want to pop something off the path part of the URL before
        the base class takes the API name off it, this is the time to do it.

        The base class implementation does nothing at all.

        :arg RESTArgs param: Input path and query arguments.
        :returns: Nothing."""
        pass


######################################################################
######################################################################
class RESTApi(MiniRESTApi):
    """REST service using :class:`~.RESTEntity` for API implementation.

    This is base class for a REST API implemented in terms of *entities*
    which support GET/PUT/POST/DELETE methods, instead of registering
    individual API methods. The general principle is to perform entity
    modelling, with express relationships, a little like databases are
    designed, then expose those entities as REST API points. This is
    usually very different from exposing just a collection of RPC APIs.

    Normally each entity represents a collection of objects, and HTTP
    methods translate to customary meanings over the collection:
    GET = query collection, PUT = insert new, POST = update existing,
    DELETE = remove item. Often the entity is written to be natively
    array-oriented so that a single PUT can insert a large number of
    objects, or single DELETE can remove many objects, for example.
    Using optional URL path arguments the entity can support operations
    on sub-items as well as collection-wide ones.

    Entities should be registered with :meth:`_add`. Where an entity API
    cannot reasonably represent the needs, raw RPC-like methods can still
    be added using :meth:`_addAPI`."""

    def _addEntities(self, entities, entry, wrapper=None):
        """Private interface for adding APIs for entities.

        :arg dict entities: See :meth:`_add` documentation.
        :arg callable entry: The pre-entry callback hook. See :meth:`_enter`.
        :arg callable wrapper: Optional wrapper to modify the method handler.
        :returns: Nothing."""
        for label, entity in viewitems(entities):
            for method in _METHODS:
                handler = getattr(entity, method.lower(), None)
                if not handler and method == 'HEAD':
                    handler = getattr(entity, 'get', None)
                if handler and getattr(handler, 'rest.exposed', False):
                    rest_args = getattr(handler, 'rest.args')
                    rest_params = getattr(handler, 'rest.params')
                    if wrapper: handler = wrapper(handler)
                    self._addAPI(method, label, handler, rest_args,
                                 [entity.validate, entry],
                                 entity=entity, **rest_params)

    def _add(self, entities):
        """Add entities.

        Adds a collection of entities and their labels. Each entity should be
        derived from :class:`~.RESTEntity` and can define a method per HTTP
        method it supports: :meth:`get` to support GET, :meth:`post` for POST,
        and so on. In addition it must provide a :meth:`validate` method
        compatible with the :class:`~.MiniRESTApi` validator convention; it
        will be called for all HTTP methods. If the entity defines :meth:`get`
        but no :meth:`head` method, :meth:`get` is automatically registered as
        the HEAD request handler as well.

        The signature of the HTTP method handlers depends on arguments copied
        to `safe` by the validation function. If :meth:`validate` copies one
        safe argument `foo` to `safe`, then :meth:`get` will be invoked with
        exactly one argument, `foo`. There is no point in specifying default
        values for HTTP method handlers as any defaults must be processed in
        :meth:`validate`. In any case it's better to keep all argument defaults
        in one place, in the :meth:`validate` method.

        :arg dict entities: A dictionary of labels with :class:`~.RESTEntity`
          value. The label is the API name and should be a bare word, with
          no slashes. It's recommended to use dash (-) as a word separator.
          The value is the entity to add, as described above.
        :returns: Nothing."""
        self._addEntities(entities, self._enter)

    def _enter(self, apiobj, method, api, param, safe):
        """Callback to be invoked after validation completes but before
        the actual API method is invoked.

        This sets the ``rest_generate_data`` and ``rest_generate_preamble``
        attributes on ``cherrypy.request``, for use later in output formatting.

        If the :func:`restcall` for the API method was called with ``generate``
        property, it's set as ``request.rest_generate_data`` for use in output
        formatting to label the name of the result object from this call. The
        default value is ``"result"``.

        The ``request.rest_generate_preamble`` is used to add a description to
        the response preamble. By default there is no description, but if the
        :func:`restcall` call for the API method used ``columns`` property, its
        value is set as ``columns`` key to the preamble. This would be used if
        the output is inherently row oriented, and the preamble gives the column
        titles and subsequent output the column values. This is the recommended
        REST API output format, especially for large output, as it is easy to
        read in clients but much more compact than emitting column labels for
        every row, e.g. by using dictionaries for each row.

        The API may modify ``request.rest_generate_preamble`` if it wishes to
        insert something to the description part.

        :arg dict apiobj: API object from :class:`~.MiniRESTApi`.
        :arg str method: HTTP method.
        :arg str api: Name of the API being called.
        :arg RESTArgs param: Incoming arguments.
        :arg RESTArgs safe: Output safe arguments.
        :returns: Nothing."""
        request.rest_generate_data = apiobj.get("generate", None)
        request.rest_generate_preamble = {}
        cols = apiobj.get("columns", None)
        if cols:
            request.rest_generate_preamble["columns"] = cols


######################################################################
######################################################################
class DBConnectionPool(Thread):
    """Asynchronous and robust database connection pool.

    .. rubric:: The pool

    This class provides a database connection pool that is thread safe
    and recovers as gracefully as possible from server side and network
    outages. Thread safety means that multiple threads may check out and
    return connections from or to the pool conncurrently. All connection
    management operations are internally transferred to a separate thread
    in order to guarantee client web server threads never block even if
    the database layer blocks or hangs in connection-related calls.

    In other words the effective purpose of this class is to guarantee
    web servers autonomously and gracefully enter a degraded state when
    the underlying database or network goes out, responding with "503
    database unavailable" but continuing to serve requests otherwise.
    Once database returns to operation the servers normally recover on
    their own without having to be manually restarted. While the main
    intent is to avoid requiring intervention on a production service,
    as a side effect this class makes database-based web services more
    usable on mobile devices with unstable network connections.

    The primary pooling purpose is to cache connections for databases
    for which connections are expensive, in particular Oracle. Instead
    of creating a new connection for each use, much of the time the pool
    returns a compatible idle connection which the application was done
    using. Connections unused for more than a pool-configured timeout are
    automatically closed.

    Between uses the connections are reset by rolling back any pending
    operations, and tested for validity by probing the server for
    "liveness" response. Connections failing tests are automatically
    disposed; when returning conections to the pool clients may indicate
    they've had trouble with it, and the pool will reclaim the connection
    instead of queuing it for reuse. In general it is hard to tell which
    errors involved connection-related issues, so it is safer to flag all
    errors on returning the connection. It is however better to filter
    out frequent benign errors such as integrity violations to avoid
    excessive connectin churn.

    Other database connection pool implementations exist, including for
    instance a session pool in the Oracle API (cx_Oracle's SessionPool).
    The reason this class implements pooling and not just validation is
    to side-step blocking or thread unsafe behaviour in the others. All
    connection management operations are passed to a worker thread. If
    the database connection layer stalls, HTTP server threads requesting
    connections will back off gracefully, reporting to their own clients
    that the database is currently unavailable. On the other hand no
    thread is able to create a connection to database if one connection
    stalls, but usually that would happen anyway with at least Oracle.

    The hand-over to the worker thread of course adds overhead, but the
    increased robustness and gracefulness in face of problems in practice
    outweighs the cost by far, and is in any case cheaper than creating
    a new connection each time. The level of overhead can be tuned by
    adjusting condition variable contention (cf. `num_signals`).

    The connections returned to clients are neither garbage collected
    nor is there a ceiling on a maximum number of connections returned.
    The client needs to be careful to `put()` as many connections as it
    received from `get()` to avoid leaking connections.

    .. rubric:: Pool specifications

    The database specification handed to the constructor should be a
    dictionary with the members:

    ``type``
      Reference to the DB API module, aka connection type.

    ``schema``
      String, name of the database schema the connection references.
      Sets connection ``current_schema`` attribute.

    ``clientid``
      String, identifies the client to session monitor, normally this
      should be `service-label@fully.qualified.domain`. Sets connection
      ``client_identifier`` attribute.

    ``liveness``
      String, SQL to execute to verify the connection remain usable,
      normally "``select sysdate from dual``" or alike. The statement
      should require a fresh response from the server on each execution
      so avoid "``select 1 from dual``" style cacheable queries.

    ``user``
      String, login user name.

    ``password``
      String, login password for ``user``.

    ``dsn``
      String, login data source name, usually the TNS entry name.

    ``timeout``
      Integer or float, number of seconds to retain unused idle
      connections before closing them. Note that this only applies to
      connections handed back to `put()`; connections given by `get()`
      but never returned to the pool are not considered idle, not even
      if the client loses the handle reference.

    ``stmtcache``
      Optional integer, overrides the default statement cache size 50.

    ``trace``
      Optional boolean flag, if set enables tracing of database activity
      for this pool, including connection ops, SQL executed, commits and
      rollbacks, and pool activity on the handles. If True, connections
      are assigned random unique labels which are used in activity log
      messages. Recycled idle connections also get a new label, but the
      connection log message will include the old label which allows
      previous logs on that connection to be found. Not to be confused
      with database server side logging; see ``session-sql`` for that.

    ``auth-role``
      Optional, (NAME, PASSWORD) string sequence. If set, connections
      acquire the database role *NAME* before use by executing the SQL
      "``set role none``", "``set role NAME identified by PASSWORD``"
      on each `get()` of the connection. In other words, if the role is
      removed or the password is changed, the client will automatically
      shed the role and fail with an error, closing the connection in
      the process, despite connection caching.

    ``session-sql``
      Optional sequence of SQL statement strings. These are executed on
      each connection `get()`. Use with session trace statements such as
      "``alter session set sql_trace = true``",
      "``alter session set events '10046 trace name context forever,
      level 12'``". It's not recommended to make any database changes.

    .. rubric:: Connection handles

    The `get()` method returns a database connection handle, a dict with
    the following members. The exact same dict needs to be returned to
    `put()` -- not a copy of it.

    ``type``
      Reference to DB API module.

    ``pool``
      Reference to this pool object.

    ``connection``
      Reference to the actual DB API connection object.

    ``trace``
      Always present, but may be either boolean False, or a non-empty
      string with the trace message prefix to use for all operations
      concerning this connection.

    .. rubric:: Attributes

    .. attribute:: connection_wait_time

       Number, the maximum time in seconds to wait for a connection to
       complete after which the client will be told the database server
       is unavailable.  This should be large enough to avoid triggering
       unnecessary database unavailability errors in sporadic delays in
       production use, but low enough to bounce clients off when the
       database server is having difficulty making progress.

       In particular client HTTP threads will be tied up this long if
       the DB server is completely hanging: completing TCP connections
       but not the full database handshake, or if TCP connection itself
       is experiencing significant delays. Hence it's important to keep
       this value low enough for the web server not to get dogged down
       or fail with time-out errors itself.

    .. attribute:: wakeup_period

       Number, maximum time in seconds to wait in the worker thread main
       loop before checking any timed out connections. The server does
       automatically adjust the wake-up time depending on work needed,
       so there usually isn't any need to change this value. The value
       should not be decreased very low to avoid an idle server from
       waking up too often.

    .. attribute:: num_signals

       Number of condition variables to use for signalling connection
       completion. The pool creates this many condition variables and
       randomly picks one to signal connection completion between the
       worker and calling threads. Increase this if there is a high
       degree of thread contention on concurrent threads waiting for
       connection completion. The default should be fine for all but
       highest connection reuse rates.

    .. attribute:: max_tries

       The maximum number of times to attempt creating a connection
       before giving up. If a connection fails tests, it is discarded
       and another attempt is made with another connection, either an
       idle one or an entirely new connection if no idle ones remain.
       This variable sets the limit on how many times to try before
       giving up. This should be high enough a value to consume any
       cached bad connections rapidly enough after network or database
       failure. Hence the pool will reclaim any bad connections at the
       maximum rate of `get()` calls times `max_tries` per
       `connection_wait_time`.

       Do not set this value to a very high value if there is a real
       possibility of major operational flukes leading to connection
       storms or account lock-downs, such as using partially incorrect
       credentials or applications with invalid/non-debugged SQL which
       cause connection to be black-listed and recycled. In other words,
       only change this parameter for applications which have undergone
       significant testing in a production environment, with clear data
       evidence the default value is not leading to sufficiently fast
       recovery after connections have started to go sour.

    .. attribute:: dbspec

       Private, the database specification given to the constructor.

    .. attribute:: id

       Private, the id given to the constructor for trace logging.

    .. attribute:: sigready

       Private, `num_signals` long list of condition variables for
       signalling connection attempt result.

    .. attribute:: sigqueue

       Private, condition variable for signalling changes to `queue`.

    .. attribute:: queue

       Private, list of pending requests to the worker thread, access
       to which is protected by `sigqueue`. Connection release requests
       go to the front of the list, connection create requests at the
       end. The worker thread takes the first request in queue, then
       executes the action with `sigqueue` released so new requests can
       be added while the worker is talking to the database.

    .. attribute:: inuse

       Private, list of connections actually handed out by `get()`. Note
       that if the client has already given up on the `get()` request by
       the time the connection is finally established, the connection is
       automatically discarded and not put no this list. This list may be
       accessed only in the worker thread as no locks are available to
       protect the access; `logstatus()` method provides the means to log
       the queue state safely in the worker thread.

    .. attribute:: idle

       Private, list of idle connections, each of which has ``expires``
       element to specify the absolute time when it will expire. The
       worker thread schedules to wake up within five seconds after the
       next earliest expire time, or in `wakeup_period` otherwise, and
       of course whenever new requests are added to `queue`. This list
       may be accessed only in the work thread as no locks are available
       to protect the access; `logstatus()` method provides the means to
       log the queue state safely in the worker thread.

    .. rubric:: Constructor

    The constructor automatically attaches this object to the cherrypy
    engine start/stop messages so the background worker thread starts or
    quits, respectively. The pool does not attempt to connect to the
    database on construction, only on the first call to `get()`, so it's
    safe to create the pool even if network or database are unavailable.

    :arg dict dbspec: Connection specification as described above.
    :arg str id: Identifier used to label trace connection messages for
                 this pool, such as the full class name of the owner."""

    connection_wait_time = 8
    wakeup_period = 60
    num_signals = 4
    max_tries = 5

    def __init__(self, id, dbspec):
        Thread.__init__(self, name=self.__class__.__name__)
        self.sigready = [Condition() for _ in range(0, self.num_signals)]
        self.sigqueue = Condition()
        self.queue = []
        self.idle = []
        self.inuse = []
        if type in dbspec and dbspec['type'].__name__ == 'MySQLdb':
            dbspec['dsn'] = dbspec['db']
        self.dbspec = dbspec
        self.id = id
        engine.subscribe("start", self.start, 100)
        engine.subscribe("stop", self.stop, 100)

    def logstatus(self):
        """Pass a request to the worker thread to log the queue status.

        It's recommended that the owner hook this method to a signal such
        as SIGUSR2 so it's possible to get the process dump its database
        connections status, especially the number of `inuse` connections,
        from outside the process.

        The request is inserted in the front of current list of pending
        requests, but do note the request isn't executed directly. If the
        worker thread is currently blocked in a database or network call,
        log output is only generated when the worker resumes control.

        :returns: Nothing."""
        self.sigqueue.acquire()
        self.queue.insert(0, (self._status, None))
        self.sigqueue.notifyAll()
        self.sigqueue.release()

    def stop(self):
        """Tell the pool to stop processing requests and to exit from the
        worker thread.

        The request is inserted in the front of current list of pending
        requests. The worker thread will react to it as soon as it's done
        processing any currently ongoing database or network call. If the
        database API layer is completely wedged, that might be never, in
        which case the application should arrange for other means to end,
        either by using a suicide alarm -- for example by calling
        signal.alarm() but not setting SIGALRM handler -- or externally
        by arranging SIGKILL to be delivered.

        Since this request is inserted in the front of pending requests,
        existing connections, whether idle or in use, will not be closed
        or even rolled back. It's assumed the database server will clean
        up the connections once the process exits.

        The constructor automatically hooks the cherrypy engine 'stop'
        message to call this method.

        :returns: Nothing."""
        self.sigqueue.acquire()
        self.queue.insert(0, (None, None))
        self.sigqueue.notifyAll()
        self.sigqueue.release()

    def get(self, id, module):
        """Get a new connection from the pool, identified to server side and
        the session monitor as to be used for action `id` by `module`.

        This retrieves the next available idle connection from the pool, or
        creates a new connection if none are available. Before handing back
        the connection, it's been tested to be actually live and usable.
        If the database connection specification included a role attribute
        or session statements, they will have been respectively set and
        executed.

        The connection request is appended to the current queue of requests.
        If the worker thread does not respond in `connection_wait_time`, the
        method gives up and indicates the database is not available. When
        that happens, the worker thread will still attempt to complete the
        connection, but will then discard it.

        :arg str id:     Identification string for this connection request.
                         This will set the ``clientinfo`` and ``action``
                         attributes on the connection for database session
                         monitoring tools to visualise and possibly remote
                         debugging of connection use.

        :arg str module: Module using this connection, typically the fully
                         qualified python class name. This will set the
                         ``module`` attribute on the connection object for
                         display in database session monitoring tools.

        :returns: A `(HANDLE, ERROR)` tuple. If a connection was successfully
                  made, `HANDLE` will contain a dict with connection data as
                  described in the class documentation and `ERROR` is `None`.
                  If no connection was made at all, returns `(None, None)`.
                  Returns `(None, (ERROBJ, TRACEBACK))` if there was an error
                  making the connection that wasn't resolved in `max_tries`
                  attempts; `ERROBJ` is the last exception thrown, `TRACEBACK`
                  the stack trace returned by `format_exc()` for it."""

        sigready = random.choice(self.sigready)
        arg = {"error": None, "handle": None, "signal": sigready,
               "abandoned": False, "id": id, "module": module}

        self.sigqueue.acquire()
        self.queue.append((self._connect, arg))
        self.sigqueue.notifyAll()
        self.sigqueue.release()

        sigready.acquire()
        now = time.time()
        until = now + self.connection_wait_time
        while True:
            dbh = arg["handle"]
            err = arg["error"]
            if dbh or err or now >= until:
                arg["abandoned"] = True
                break
            sigready.wait(until - now)
            now = time.time()
        sigready.release()
        return dbh, err

    def put(self, dbh, bad=False):
        """Add a database handle `dbh` back to the pool.

        Normally `bad` would be False and the connection is added back to
        the pool as an idle connection, and will be reused for a subsequent
        connection.

        Any pending operations on connections are automatically cancalled
        and rolled back before queuing them into the idle pool. These will
        be executed asynchronously in the database connection worker thread,
        not in the caller's thread. However note that if the connection
        became unusable, attempting to roll it back may block the worker.
        That is normally fine as attempts to create new connections will
        start to fail with timeout, leading to "database unavailable" errors.

        If the client has had problems with the connection, it should most
        likely set `bad` to True, so the connection will be closed and
        discarded. It's safe to reuse connections after benign errors such
        as basic integrity violations. However there are a very large class
        of obscure errors which actually mean the connection handle has
        become unusable, so it's generally safer to flag the handle invalid
        on error -- with the caveat that errors should be rare to avoid
        excessive connection churn.

        :arg dict dbh: A database handle previously returned by `get()`. It
                       must be the exact same dict object, not a copy.
        :arg bool bad: If True, `dbh` is likely bad, so please try close it
                       instead of queuing it for reuse.
        :returns: Nothing."""

        self.sigqueue.acquire()
        self.queue.insert(0, ((bad and self._disconnect) or self._release, dbh))
        self.sigqueue.notifyAll()
        self.sigqueue.release()

    def run(self):
        """Run the connection management thread."""

        # Run forever, pulling work from "queue". Round wake-ups scheduled
        # from timeouts to five-second quantum to maximise the amount of
        # work done per round of clean-up and reducing wake-ups.
        next = self.wakeup_period
        while True:
            # Whatever reason we woke up, even if sporadically, process any
            # pending requests first.
            self.sigqueue.acquire()
            self.sigqueue.wait(max(next, 5))
            while self.queue:
                # Take next action and execute it. "None" means quit. Release
                # the queue lock while executing actions so callers can add
                # new requests, e.g. release connections while we work here.
                # The actions are not allowed to throw any exceptions.
                action, arg = self.queue.pop(0)
                self.sigqueue.release()
                if action:
                    action(arg)
                else:
                    return
                self.sigqueue.acquire()
            self.sigqueue.release()

            # Check idle connections for timeout expiration. Calculate the
            # next wake-up as the earliest expire time, but note that it
            # gets rounded to minimum five seconds above to scheduling a
            # separate wake-up for every handle. Note that we may modify
            # 'idle' while traversing it, so need to clone it first.
            now = time.time()
            next = self.wakeup_period
            for old in self.idle[:]:
                if old["expires"] <= now:
                    self.idle.remove(old)
                    self._disconnect(old)
                else:
                    next = min(next, old["expires"] - now)

    def _status(self, *args):
        """Action handler to dump the queue status."""
        cherrypy.log("DATABASE CONNECTIONS: %s@%s %s: timeout=%d inuse=%d idle=%d"
                     % (self.dbspec["user"], self.dbspec["dsn"], self.id,
                        self.dbspec["timeout"], len(self.inuse), len(self.idle)))

    def _error(self, title, rest, err, where):
        """Internal helper to generate error message somewhat similar to
        :func:`~.report_rest_error`.

        :arg str title: All-capitals error message title part.
        :arg str rest: Possibly non-empty trailing error message part.
        :arg Exception err: Exception object reference.
        :arg str where: Traceback for `err` as returned by :ref:`format_exc()`."""
        errid = "%032x" % random.randrange(1 << 128)
        cherrypy.log("DATABASE THREAD %s ERROR %s@%s %s %s.%s %s%s (%s)"
                     % (title, self.dbspec["user"], self.dbspec["dsn"], self.id,
                        getattr(err, "__module__", "__builtins__"),
                        err.__class__.__name__, errid, rest, str(err).rstrip()))
        for line in where.rstrip().split("\n"):
            cherrypy.log("  " + line)

    def _connect(self, req):
        """Action handler to fulfill a connection request."""
        s = self.dbspec
        dbh, err = None, None

        # If tracing, issue log line that identifies this connection series.
        trace = s["trace"] and ("RESTSQL:" + "".join(random.sample(letters, 12)))
        trace and cherrypy.log("%s ENTER %s@%s %s (%s) inuse=%d idle=%d" %
                               (trace, s["user"], s["dsn"], self.id, req["id"],
                                len(self.inuse), len(self.idle)))

        # Attempt to connect max_tries times.
        for _ in range(0, self.max_tries):
            try:
                # Take next idle connection, or make a new one if none exist.
                # Then test and prepare that connection, linking it in trace
                # output to any previous uses of the same object.
                dbh = (self.idle and self.idle.pop()) or self._new(s, trace)
                assert dbh["pool"] == self
                assert dbh["connection"]
                prevtrace = dbh["trace"]
                dbh["trace"] = trace
                self._test(s, prevtrace, trace, req, dbh)

                # The connection is ok. Kill expire limit and return this one.
                if "expires" in dbh:
                    del dbh["expires"]
                break
            except Exception as e:
                # The connection didn't work, report and remember this exception.
                # Note that for every exception reported for the server itself
                # we may report up to max_tries exceptions for it first. That's
                # a little verbose, but it's more useful to have all the errors.
                err = (e, format_exc())
                self._error("CONNECT", "", *err)
                dbh and self._disconnect(dbh)
                dbh = None

        # Return the result, and see if the caller abandoned this attempt.
        req["signal"].acquire()
        req["error"] = err
        req["handle"] = dbh
        abandoned = req["abandoned"]
        req["signal"].notifyAll()
        req["signal"].release()

        # If the caller is known to get our response, record the connection
        # into 'inuse' list. Otherwise discard any connection we made.
        if not abandoned and dbh:
            self.inuse.append(dbh)
        elif abandoned and dbh:
            cherrypy.log("DATABASE THREAD CONNECTION ABANDONED %s@%s %s"
                         % (self.dbspec["user"], self.dbspec["dsn"], self.id))
            self._disconnect(dbh)

    def _new(self, s, trace):
        """Helper function to create a new connection with `trace` identifier."""
        trace and cherrypy.log("%s instantiating a new connection" % trace)
        ret = {"pool": self, "trace": trace, "type": s["type"]}
        if s['type'].__name__ == 'MySQLdb':
            ret.update({"connection": s["type"].connect(s['host'], s["user"], s["password"], s["db"], int(s["port"]))})
        else:
            ret.update({"connection": s["type"].connect(s["user"], s["password"], s["dsn"], threaded=True)})

        return ret

    def _test(self, s, prevtrace, trace, req, dbh):
        """Helper function to prepare and test an existing connection object."""
        # Set statement cache. Default is 50 statments but spec can override.
        c = dbh["connection"]
        c.stmtcachesize = s.get("stmtcache", 50)

        # Emit log message to identify this connection object. If it was
        # previously used for something else, log that too for detailed
        # debugging involving problems with connection reuse.
        if s['type'].__name__ == 'MySQLdb':
            client_version = s["type"].get_client_info()
            version = ".".join(str(x) for x in s["type"].version_info)
        else:
            client_version = ".".join(str(x) for x in s["type"].clientversion())
            version = c.version
        prevtrace = ((prevtrace and prevtrace != trace and
                      " (previously %s)" % prevtrace.split(":")[1]) or "")
        trace and cherrypy.log("%s%s connected, client: %s, server: %s, stmtcache: %d"
                               % (trace, prevtrace, client_version,
                                  version, c.stmtcachesize))

        # Set the target schema and identification attributes on this one.
        c.current_schema = s["schema"]
        c.client_identifier = s["clientid"]
        c.clientinfo = req["id"]
        c.module = req["module"]
        c.action = req["id"][:32]

        # Ping the server. This will detect some but not all dead connections.
        trace and cherrypy.log("%s ping" % trace)
        c.ping()

        # At least server responded, now try executing harmless SQL but one
        # that requires server to actually respond. This detects remaining
        # bad connections.
        trace and cherrypy.log("%s check [%s]" % (trace, s["liveness"]))
        c.cursor().execute(s["liveness"])

        # If the pool requests authentication role, set it now. First reset
        # any roles we've acquired before, then attempt to re-acquire the
        # role. Hence if the role is deleted or its password is changed by
        # application admins, we'll shed any existing privileges and close
        # the connection right here. This ensures connection pooling cannot
        # be used to extend role privileges forever.
        if "auth-role" in s:
            trace and cherrypy.log("%s set role none")
            c.cursor().execute("set role none")
            trace and cherrypy.log("%s set role %s" % (trace, s["auth-role"][0]))
            c.cursor().execute("set role %s identified by %s" % s["auth-role"])

        # Now execute session statements, e.g. tracing event requests.
        if "session-sql" in s:
            for sql in s["session-sql"]:
                trace and cherrypy.log("%s session-sql [%s]" % (trace, sql))
                c.cursor().execute(sql)

        # OK, connection's all good.
        trace and cherrypy.log("%s connection established" % trace)

    def _release(self, dbh):
        """Action handler to release a connection back to the pool."""
        try:
            # Check the handle didn't get corrupted.
            assert dbh["pool"] == self
            assert dbh["connection"]
            assert dbh in self.inuse
            assert dbh not in self.idle
            assert "expires" not in dbh

            # Remove from 'inuse' list first in case the rest throws/hangs.
            s = self.dbspec
            trace = dbh["trace"]
            self.inuse.remove(dbh)

            # Roll back any started transactions. Note that we don't want to
            # call cancel() on the connection here as it will most likely just
            # degenerate into useless "ORA-25408: can not safely replay call".
            trace and cherrypy.log("%s release with rollback" % trace)
            dbh["connection"].rollback()

            # Record expire time and put to end of 'idle' list; _connect()
            # takes idle connections from the back of the list, so we tend
            # to reuse most recently used connections first, and to prune
            # the number of connections in use to the minimum.
            dbh["expires"] = time.time() + s["timeout"]
            self.idle.append(dbh)
            trace and cherrypy.log("%s RELEASED %s@%s timeout=%d inuse=%d idle=%d"
                                   % (trace, s["user"], s["dsn"], s["timeout"],
                                      len(self.inuse), len(self.idle)))
        except Exception as e:
            # Something went wrong, nuke the connection from orbit.
            self._error("RELEASE", " failed to release connection", e, format_exc())

            try:
                self.inuse.remove(dbh)
            except ValueError:
                pass

            try:
                self.idle.remove(dbh)
            except ValueError:
                pass

            self._disconnect(dbh)

    def _disconnect(self, dbh):
        """Action handler to discard the connection entirely."""
        try:
            # Assert internal consistency invariants; the handle may be
            # marked for use in case it's discarded with put(..., True).
            assert dbh not in self.idle

            try:
                self.inuse.remove(dbh)
            except ValueError:
                pass

            # Close the connection.
            s = self.dbspec
            trace = dbh["trace"]
            trace and cherrypy.log("%s disconnecting" % trace)
            dbh["connection"].close()

            # Remove references to connection object as much as possible.
            del dbh["connection"]
            dbh["connection"] = None

            # Note trace that this is now gone.
            trace and cherrypy.log("%s DISCONNECTED %s@%s timeout=%d inuse=%d idle=%d"
                                   % (trace, s["user"], s["dsn"], s["timeout"],
                                      len(self.inuse), len(self.idle)))
        except Exception as e:
            self._error("DISCONNECT", " (ignored)", e, format_exc())


######################################################################
######################################################################
class DatabaseRESTApi(RESTApi):
    """A :class:`~.RESTApi` whose entities represent database contents.

    This class wraps API calls into an environment which automatically sets
    up and tears down a database connection around the call, and translates
    all common database errors such as unique constraint violations into an
    appropriate and meaningful :class:`~.RESTError`-derived error.

    This class is fundamentally instance-aware. The :meth:`_precall` hook is
    used to pop a required instance argument off the URL, before the API name,
    so URLs look like ``/app/prod/entity``. The instance name from the URL is
    used to select the database instance on which the API operates, and the
    HTTP method is used to select suitable connection pool, for example reader
    account for GET operations and a writer account for PUT/POST/DELETE ones.

    Normally the entities do not use database connection directly, but use
    the convenience functions in this class. These methods perform many
    utility tasks needed for high quality implementation, plus make it very
    convenient to write readable REST entity implementations which conform
    to the preferred API conventions. For example the :meth:`query` method
    not only executes a query, but it runs a SQL statement cleaner, does all
    the SQL execution trace logging, keeps track of last statement executed
    with its binds (to be logged with error output in case an exception is
    raised later on), fills ``rest_generate_preamble["columns"]`` from the
    SQL columns titles, and returns a generator over the result, optionally
    filtering the results by a regular expression match criteria.

    The API configuration must include ``db`` value, whose value should be
    the qualified name of the python object for the multi-instance database
    authentication specification. The object so imported should yield a
    dictionary whose keys are the supported instance names. The values should
    be dictionaries whose keys are HTTP methods and values are dictionaries
    specifying database contact details for that use; "*" can be used as a
    key for "all other methods". For example the following would define two
    instances, "prod" and "dev", with "prod" using a reader account for GETs
    and a writer account for other methods, and "dev" with a single account::

      DB = { "prod": { "GET": { "user": "foo_reader", ... },
                       "*":   { "user": "foo_writer", ... } },
             "dev":  { "*":   { "user": "foo", ... } } }

    The class automatically uses a thread-safe, resilient connection pool
    for the database connections (cf. :class:`~.DBConnectionPool`). New
    database connections are created as needed on demand. If the database is
    unavailable, the server will gracefully degrade to reporting HTTP "503
    Service Unavailable" status code. Database availability is not required
    on server start-up, connections are first attempted on the first HTTP
    request which requires a connection. In fact connections are prepared
    only *after* request validation has completed, mainly as a security
    measure to prevent unsafe parameter validation code from mixing up with
    database query execution. At the end of API method execution connection
    handles are automatically rolled back, so API should explicitly commit
    if it wants any changes made to last. Sending the server SIGUSR2 signal
    will log connection usage statistics and pool timeouts.

    .. rubric:: Attributes

    .. attribute:: _db

       The database configuration imported from ``self.config.db``. Each
       connection specification has dictionary key ``"pool"`` added,
       pointing to the :class:`~.DBConnectionPool` managing that pool.

    .. rubric:: Constructor

    Constructor arguments are the same as for the base class.
    """
    _ALL_POOLS = []

    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)
        signal.signal(signal.SIGUSR2, self._logconnections)
        modname, item = config.db.rsplit(".", 1)
        module = __import__(modname, globals(), locals(), [item])
        self._db = getattr(module, item)
        myid = "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        for spec in viewvalues(self._db):
            for db in viewvalues(spec):
                if isinstance(db, dict):
                    db["pool"] = DBConnectionPool(myid, db)
                    DatabaseRESTApi._ALL_POOLS.append(db["pool"])

    @staticmethod
    def _logconnections(*args):
        """SIGUSR2 signal handler to log status of all pools."""
        list([p.logstatus() for p in DatabaseRESTApi._ALL_POOLS])

    def _add(self, entities):
        """Add entities.

        See base class documentation for the semantics. This wraps the API
        method with :meth:`_wrap` to handle database related errors, and
        inserts :meth:`_dbenter` as the last validator in chain to wrap
        the method in database connection management. :meth:`_dbenter`
        will install :meth:`_dbexit` as a request clean-up callback to
        handle connection disposal if database connection succeeds."""
        self._addEntities(entities, self._dbenter, self._wrap)

    def _wrap(self, handler):
        """Internal helper function to wrap calls to `handler` inside a
        database exception filter. Any exceptions raised will be passed to
        :meth:`_dberror`."""

        @wraps(handler)
        def dbapi_wrapper(*xargs, **xkwargs):
            try:
                return handler(*xargs, **xkwargs)
            except Exception as e:
                self._dberror(e, format_exc(), False)

        return dbapi_wrapper

    def _dberror(self, errobj, trace, inconnect):
        """Internal helper routine to process exceptions while inside
        database code.

        This method does necessary housekeeping to roll back and dispose any
        connection already made, remember enough of the context for error
        reporting, and convert the exception into a relevant REST error type
        (cf. :class:`~.RESTError`). For example constraint violations turn
        into :class:`~.ObjectAlreadyExists` or :class:`~.MissingObject`.

        Errors not understood become :class:`~.DatabaseExecutionError`, with
        the SQL and bind values of last executed statement reported into the
        server logs (but not to the client). Note that common programming errors,
        :class:`~.KeyError`, :class:`~.ValueError` or :class:`~.NameError`
        raised in API implementation, become :class:`~.DatabaseExecutionError`
        too. This will make little difference to the client, and will still
        include all the relevant useful information in the server logs, but
        of course they are not database-related as such.

        :arg Exception errobj: The exception object.
        :arg str trace: Associated traceback as returned by :func:`format_exc`.
        :arg bool inconnect: Whether exception occurred while trying to connect.
        :return: Doesn't, raises an exception."""

        # Grab last sql executed and whatever binds were used so far,
        # the database type object, and null out the rest so that
        # post-request and any nested error handling will ignore it.
        db = request.db
        type = db["type"]
        instance = db["instance"]
        sql = (db["last_sql"],) + db["last_bind"]

        # If this wasn't connection failure, just release the connection.
        # If that fails, force drop the connection. We ignore errors from
        # this since we are attempting to report another earlier error.
        db["handle"] and db["pool"].put(db["handle"], True)

        # Set the db backend
        DB_BACKEND = db['type'].__name__

        del request.db
        del db

        # Raise an error of appropriate type.
        errinfo = {"errobj": errobj, "trace": trace}
        dberrinfo = {"errobj": errobj, "trace": trace,
                     "lastsql": sql, "instance": instance}
        if inconnect:
            raise DatabaseUnavailable(**dberrinfo)
        elif isinstance(errobj, type.IntegrityError):
            errorcode = errobj[0] if DB_BACKEND == 'MySQLdb' else errobj.args[0].code
            if errorcode in {'cx_Oracle': (1, 2292), 'MySQLdb': (1062,)}[DB_BACKEND]:
                # ORA-00001: unique constraint (x) violated
                # ORA-02292: integrity constraint (x) violated - child record found
                # MySQL: 1062, Duplicate entry 'x' for key 'y'
                # MySQL: Both unique and integrity constraint falls into 1062 error
                raise ObjectAlreadyExists(**errinfo)
            elif errorcode in {'cx_Oracle': (1400, 2290), 'MySQLdb': (1048,)}[DB_BACKEND]:
                # ORA-01400: cannot insert null into (x)
                # ORA-02290: check constraint (x) violated
                # MySQL: 1048, Column (x) cannot be null
                # There are no check constraint in MySQL. Oracle 2290 equivalent does not exist
                raise InvalidParameter(**errinfo)
            elif errorcode == {'cx_Oracle': 2291, 'MySQLdb': 1452}[DB_BACKEND]:
                # ORA-02291: integrity constraint (x) violated - parent key not found
                # MySQL: 1452, Cannot add or update a child row: a foreign key constraint fails
                raise MissingObject(**errinfo)
            else:
                raise DatabaseExecutionError(**dberrinfo)
        elif isinstance(errobj, type.OperationalError):
            raise DatabaseUnavailable(**dberrinfo)
        elif isinstance(errobj, type.InterfaceError):
            raise DatabaseConnectionError(**dberrinfo)
        elif isinstance(errobj, (HTTPRedirect, RESTError)):
            raise
        else:
            raise DatabaseExecutionError(**dberrinfo)

    def _precall(self, param):
        """Pop instance from the URL path arguments before :meth:`_call` takes
        entity name from it.

        This orders URL to have entities inside an instance, which is logical.
        Checks the instance name is present and one of the known databases as
        registered to the constructor into :attr:`_db`, and matches the HTTP
        method to the right database connection pool.

        If the instance argument is missing, unsafe, or unknown, raises an
        :class:`~.NoSuchInstance` exception. If there is no connection pool
        for that combination of HTTP method and instance, raises an
        :class:`~.DatabaseUnavailable` exception.

        If successful, remembers the database choice in ``cherrypy.request.db``
        but does not yet attempt connection to it. This is a dictionary with
        the following fields.

        ``instance``
          String, the instance name.

        ``type``
          Reference to the DB API module providing database connection.

        ``pool``
          Reference to the :class:`~.DBConnectionPool` providing connections.

        ``handle``
          Reference to the database connection handle from the pool. Initially
          set to `None`.

        ``last_sql``
          String, the last SQL statement executed on this connection. Initially
          set to `None`, filled in by the statement execution utility function
          :meth:`prepare` (which :meth:`execute` and :meth:`executemany` call).
          Used for error reporting in :meth:`_dberror`.

        ``last_bind``
          A tuple *(values, keywords)* of the last bind values used on this
          connection. Initially set to *(None, None)* and reset back to that
          value by each call to :meth:`prepare`, filled in by the statement
          execution utility functions :meth:`executemany` and :meth:`execute`.
          Use for error reporting in :meth:`_dberror`. Note that if the data
          is security sensitive, or extremely voluminous with thousands of
          values, this might cause some concern.

        :arg RESTArgs param: Incoming URL path and query arguments. If a valid
          instance name is found, pops the first path item off ``param.args``.
        :returns: Nothing."""

        # Check we were given an instance argument and it's known.
        if not param.args or param.args[0] not in self._db:
            raise NoSuchInstance()
        if not re.match(r"^[a-z]+$", param.args[0]):
            raise NoSuchInstance("Invalid instance name")

        instance = param.args.pop(0)

        # Get database object.
        if request.method in self._db[instance]:
            db = self._db[instance][request.method]
        elif request.method == 'HEAD' and 'GET' in self._db[instance]:
            db = self._db[instance]['GET']
        elif '*' in self._db[instance]:
            db = self._db[instance]['*']
        else:
            raise DatabaseUnavailable()

        # Remember database instance choice, but don't do anything about it yet.
        request.db = {"instance": instance, "type": db["type"], "pool": db["pool"],
                      "handle": None, "last_sql": None, "last_bind": (None, None)}

    def _dbenter(self, apiobj, method, api, param, safe):
        """Acquire database connection just before invoking the entity.

        :meth:`_add` arranges this method to be invoked as the last validator
        function, just before the base class calls the actual API method. We
        acquire the database connection here, and arrange the connection to
        be released by :meth:`_dbexit` after the response has been generated.

        It is not possible to release the database connection right after the
        response handler returns, as it is normally a generator which is still
        holding on to a database cursor for streaming out the response. Here we
        register connection release to happen in the CherryPy request clean-up
        phase ``on_end_request``. Clean-up is ensured even if errors occurred.

        Database connections will be identified by the HTTP method, instance
        name and entity name, and with the fully qualified class name of the
        entity handling the request. This allows server activity to be more
        usefully monitored in the database session monitor tools.

        Raises a :class:`~.DatabaseUnavailable` exception if connecting to the
        database fails, either the connection times out or generates errors.
        This exception is translated into "503 Service Unavailable" HTTP status.
        Note that :class:`~.DBConnectionPool` verifies the connections are
        actually usable before handing them back, so connectivity problems are
        normally all caught here, with the server degrading into unavailability.

        A successfully made connection is recorded in ``request.db["handle"]``
        and ``request.rest_generate_data`` is initialised from the ``generate``
        :func:`restcall` parameter, as described in :meth:`.RESTApi._enter`.
        ``request.rest_generate_preamble`` is reset to empty; it will normally
        be filled in by :meth:`execute` and :meth:`executemany` calls.

        :args: As documented for validation functions.
        :returns: Nothing."""

        assert getattr(request, "db", None), "Expected DB args from _precall"
        assert isinstance(request.db, dict), "Expected DB args from _precall"

        # Get a pool connection to the instance.
        request.rest_generate_data = None
        request.rest_generate_preamble = {}
        module = "%s.%s" % (apiobj['entity'].__class__.__module__,
                            apiobj['entity'].__class__.__name__)
        id = "%s %s %s" % (method, request.db["instance"], api)
        dbh, err = request.db["pool"].get(id, module)

        if err:
            self._dberror(err[0], err[1], True)
        elif not dbh:
            del request.db
            raise DatabaseUnavailable()
        else:
            request.db["handle"] = dbh
            request.rest_generate_data = apiobj.get("generate", None)
            request.hooks.attach('on_end_request', self._dbexit, failsafe=True)

    def _dbexit(self):
        """CherryPy request clean-up handler to dispose the database connection.

        Releases the connection back to the :class:`~.DBConnectionPool`. As
        described in :meth:`_dbenter`, call to this function is arranged at
        the end of the request processing inside CherryPy.

        :returns: Nothing."""

        if getattr(request, "db", None) and request.db["handle"]:
            request.db["pool"].put(request.db["handle"], False)

    def sqlformat(self, schema, sql):
        """Database utility function which reformats the SQL statement by
        removing SQL ``--`` comments and leading and trailing white space
        on lines, and converting multi-line statements to a single line.

        The main reason this function is used is that often SQL statements
        are written in code as multi-line quote blocks, and it's awkward
        to include them in database tracing and error logging output. It
        is much more readable to have single line statements with canonical
        spacing, so we put all executed statements into that form before use.

        If `schema` is provided, the statement is munged to insert its value
        as a schema prefix to all tables, sequences, indexes and keys that
        are not already prefixed by a schema name. This is not normally used,
        the preferred mechanism is to set ``current_schema`` attribute on the
        connections as per :class:`~.DBConnectionPool` documentation. For the
        munging to work, it is assumed all table names start with ``t_``, all
        sequence names start with ``seq_``, all index names start with ``ix_``,
        all primary key names start with ``pk_`` and all foreign key names
        start with ``fk_``.

        This method does not remove ``/* ... */`` comments to avoid removing
        any deliberate query hints.

        :arg str sql: SQL statement to clean up.
        :arg str schema: If not `None`, schema prefix to insert.
        :returns: Cleaned up SQL statement string."""

        sql = re.sub(r"--.*", "", sql)
        sql = re.sub(r"^\s+", "", sql)
        sql = re.sub(r"\s+$", "", sql)
        sql = re.sub(r"\n\s+", " ", sql)
        if schema:
            sql = re.sub(r"(?<=\s)((t|seq|ix|pk|fk)_[A-Za-z0-9_]+)(?!\.)",
                         r"%s.\1" % schema, sql)
            sql = re.sub(r"(?<=\s)((from|join)\s+)([A-Za-z0-9_]+)(?=$|\s)",
                         r"\1%s.\3" % schema, sql)
        return sql

    def prepare(self, sql):
        """Prepare a SQL statement.

        This utility cleans up SQL statement `sql` with :meth:`sqlformat`,
        obtains a new cursor from the current database connection, and
        calls the ``prepare()`` on it, then returns the cursor.

        The cleaned up statement is remembered as ``request.db["last_sql"]``
        and if database tracing is enabled, logged for the current connection.
        In log output, sensitive contents like passwords are censored out
        (cf. :obj:`~._RX_CENSOR`). The binds in ``request.db["last_bind"]``
        are reset to (None, None).

        This method can be called only between :meth:`_dbenter` and
        :meth:`_dbexit`.

        :arg str sql: SQL statement.
        :returns: Cursor on which statement was prepared."""

        assert request.db["handle"], "DB connection missing"
        sql = self.sqlformat(None, sql)  # FIXME: schema prefix?
        trace = request.db["handle"]["trace"]
        logsql = re.sub(_RX_CENSOR, r"\1 <censored>", sql)
        request.db["last_bind"] = None, None
        request.db["last_sql"] = logsql
        trace and cherrypy.log("%s prepare [%s]" % (trace, logsql))
        c = request.db["handle"]["connection"].cursor()
        if request.db['type'].__name__ == 'MySQLdb':
            return c
        c.prepare(sql)
        return c

    def execute(self, sql, *binds, **kwbinds):
        """Execute a SQL statement with bind variables.

        This method mirrors the DB API :func:`execute` method on cursors. It
        executes the `sql` statement after invoking :meth:`prepare` on it,
        saving the bind variables into ``request.db["last_bind"]`` and
        logging the binds if tracing is enabled. It returns both the cursor
        and the return value from the underlying DB API call as a tuple.

        The convention for passing binds is the same as for the corresponding
        DB API :func:`execute` method on cursors. You may find :meth:`bindmap`
        useful to convert dict-of-lists keyword arguments of a REST entity to
        the commonly used list-of-dicts to `binds` argument for this method.

        This method can be called only between :meth:`_dbenter` and
        :meth:`_dbexit`.

        :arg str sql: SQL statement string.
        :arg list binds: Positional binds.
        :arg dict kwbinds: Keyword binds.
        :returns: Tuple *(cursor, variables)*, where *cursor* is the cursor
          returned by :meth:`prepare` for the SQL statement, and *variables*
          is the list of variables if `sql` is a query, or None, as returned
          by the corresponding DB API :func:`execute` method."""

        c = self.prepare(sql)
        trace = request.db["handle"]["trace"]
        request.db["last_bind"] = (binds, kwbinds)
        trace and cherrypy.log("%s execute: %s %s" % (trace, binds, kwbinds))
        if request.db['type'].__name__ == 'MySQLdb':
            return c, c.execute(sql, kwbinds)
        return c, c.execute(None, *binds, **kwbinds)

    def executemany(self, sql, *binds, **kwbinds):
        """Execute a SQL statement many times with bind variables.

        This method mirrors the DB DBI :func:`executemany` method on cursors.
        It executes the statement over a sequence of bind values; otherwise
        it is the same as :meth:`execute`.

        :args: See :meth:`execute`.
        :returns: See :meth:`execute`."""

        c = self.prepare(sql)
        trace = request.db["handle"]["trace"]
        request.db["last_bind"] = (binds, kwbinds)
        trace and cherrypy.log("%s executemany: %s %s" % (trace, binds, kwbinds))
        if request.db['type'].__name__ == 'MySQLdb':
            return c, c.executemany(sql, binds[0])
        return c, c.executemany(None, *binds, **kwbinds)

    def query(self, match, select, sql, *binds, **kwbinds):
        """Convenience function to :meth:`execute` a query, set ``"columns"`` in
        the REST response preamble to the column titles of the query result, and
        return a generator over the cursor as a result, possibly filtered.

        The SQL statement is expected to be a SELECT query statement. The `sql`,
        `binds` and `kwbinds` are passed to :meth:`execute`. The returned cursor's
        ``description``, the column titles of the returned rows, are used to set
        ``request.rest_generate_preamble["columns"]`` description preamble. The
        assumption is those column titles are compatible with the argument names
        accepted by REST methods such that "columns" returned here are compatible
        with the argument names accepted by the GET/PUT/POST/DELETE methods.

        If `match` is not `None`, it is assumed to be a regular expression object
        to be passed to :func:`rxfilter` to filter results by column value, and
        `select` should be a callable which returns the value to filter on given
        the row values, typically an :func:`operator.itemgetter`.  If `match` is
        `None`, this function returns the trivial generator :func:`rows`.

        The convention for passing binds is the same as for the corresponding
        DB API :func:`execute` method on cursors. You may find :meth:`bindmap`
        useful to convert dict-of-lists keyword arguments of a REST entity to
        the commonly used list-of-dicts to `binds` argument for this method.

        This method can be called only between :meth:`_dbenter` and
        :meth:`_dbexit`.

        :arg re.RegexObject match: Either `None` or a regular expression for
          filtering the results by a colum value.
        :arg callable select: If `match` is not `None`, a callable which
          retrieves the column value to run `match` against. It must return
          a string value. It would normally be :func:`operator.itemgetter`,
          may need additional code if type conversion is needed, or if the
          caller wants to join values of multiple columns for filtering.
        :arg str sql: SQL statement string.
        :arg list binds: Positional binds.
        :arg dict kwbinds: Keyword binds.
        :returns: A generator over the query results."""

        c, _ = self.execute(sql, *binds, **kwbinds)
        request.rest_generate_preamble["columns"] = \
            [x[0].lower() for x in c.description]
        if match:
            return rxfilter(match, select, c)
        else:
            return rows(c)

    def modify(self, sql, *binds, **kwbinds):
        """Convenience function to :meth:`executemany` a query, and return a
        a result with the number of affected rows, after verifying it matches
        exactly the number of inputs given and committing the transaction.

        The SQL statement is expected to be a modifying query statement: INSERT,
        UPDATE, DELETE, MERGE, and so on. If `binds` is non-empty, it is passed
        as-is to :meth:`executemany` with `kwbinds`, and the expected number of
        rows to be affected is set to *len(binds[0]).* Otherwise it's assumed
        arguments are given as lists by keyword, and they are transposed to a
        list of dictionaries with :meth:`bindmap`; for REST methods this is
        expected to be the most common calling convention since REST API calls
        themselves will receive arguments as lists by keyword argument. The
        expected number of affected rows is set to the length of the list
        returned by :meth:`bindmap`; note that :meth:`bindmap` requires all
        arguments to have the same number of values.

        Since :meth:`executemany` is used, the operation is intrinsically array
        oriented. This property is useful for making REST methods natively
        collection oriented, allowing operations to act efficiently on large
        quantities of input data with very little special coding.

        After executing the statement but before committing, this method runs
        :meth:`rowstatus` to check that the number of rows affected matches
        exactly the number of inputs. :meth:`rowstatus` throws an exception
        if that is not the case, and otherwise returns a trivial result object
        of the form ``[{ "modified": rowcount }]`` meant to be returned to REST
        API callers.

        Once :meth:`rowstatus` is happy with the result, the transaction is
        committed, with appropriate debug output if trace logging is enabled.

        :arg str sql: SQL modify statement.
        :arg list binds: Bind variables by position: list of dictionaries.
        :arg dict kwbinds: Bind variables by keyword: dictionary of lists.
        :result: See :meth:`rowstatus` and description above."""

        if binds:
            c, _ = self.executemany(sql, *binds, **kwbinds)
            expected = len(binds[0])
        else:
            kwbinds = self.bindmap(**kwbinds)
            c, _ = self.executemany(sql, kwbinds, *binds)
            expected = len(kwbinds)
        result = self.rowstatus(c, expected)
        trace = request.db["handle"]["trace"]
        trace and cherrypy.log("%s commit" % trace)
        request.db["handle"]["connection"].commit()
        return result

    def rowstatus(self, c, expected):
        """Verify the last statement executed on cursor `c` touched exactly
        `expected` number of rows.

        The last SQL statement executed on `c` is assumed to have been a
        modifying such as INSERT, DELETE, UPDATE or MERGE. If the number of
        rows affected by the statement is less than `expected`, raises a
        :class:`~.MissingObject` exception. If the number is greater than
        `expected`, raises a :class:`~.TooManyObjects` exception. If the
        number is exactly right, returns a generator over a trivial result
        object of the form ``[{ "modified": rowcount }]`` which is meant to
        be returned as the result from the underlying REST method.

        Normally you would use this method via :meth:`modify`, but it can be
        called directly to verify each step of multi-statement update operation.

        This method exists so that a REST entity can be written easily to have
        a documented invariant, which is likely to always pass, and needs to
        be checked with very low overhead -- aka each modify operation touches
        the exact number of database objects requested, and attempts to act on
        non-existent objects are caught and properly reported to clients. It
        enables trivially one to use ``INSERT INTO ... SELECT ...`` statement
        style for insertions, deletions and updates. If the SELECT part returns
        fewer or more results than expected, an exception will be raised and the
        transaction rolled back even if the schema constraints do not catch the
        problem.

        In other words the check here makes sure that any app logic or database
        schema constraint mistakes turn into hard API errors rather than making
        hash out of the database contents, or lying to clients that an operation
        succeeded when it did not actually fully perform the requested task. It
        eliminates the need to proliferate result checking code through the app
        and to worry about a large class of possible race conditions in clustered
        web services talking to databases.

        This method can be called on any valid cursor object. It's in no way
        dependent on the use or non-use of the other database utility methods.

        :arg c: DB API cursor object.
        :arg int expected: Number of rows expected to be affected.
        :returns: See description above."""

        if c.rowcount < expected:
            raise MissingObject(info="%d vs. %d expected" % (c.rowcount, expected))
        elif c.rowcount > expected:
            raise TooManyObjects(info="%d vs. %d expected" % (c.rowcount, expected))
        return rows([{"modified": c.rowcount}])

    def bindmap(self, **kwargs):
        """Given `kwargs` of equal length list keyword arguments, returns the
        data transposed as list of dictionaries each of which has a value for
        every key from each of the lists.

        This method is convenient for arranging HTTP request keyword array
        parameters for bind arrays suitable for `executemany` call.

        For example the call ``api.bindmap(a = [1, 2], b = [3, 4])`` returns a
        list of dictionaries ``[{ "a": 1, "b": 3 }, { "a": 2, "b": 4 }]``."""

        keys = list(kwargs)
        return [dict(list(zip(keys, vals))) for vals in zip(*listvalues(kwargs))]


######################################################################
######################################################################
class RESTEntity(object):
    """Base class for entities in :class:`~.RESTApi`-based interfaces.

    This class doesn't offer any service other than holding on to the
    arguments given in the constructor. The derived class must implement at
    least a :meth:`validate` method, and some number of HTTP request method
    functions as described in :class:`~.RESTApi` documentation.

    Do note that keyword arguments with defaults are pointless on HTTP method
    handlers: the methods are never called in a fashion that would actually
    use the declaration defaults. Specifically, every argument stored into
    ``safe.kwargs`` by a validator will always be given *some* value. If the
    parameter was declared optional to the validator, it will automatically
    be given `None` value when not present in the request arguments.

    .. rubric:: Attributes

    .. attribute:: app

       Reference to the :class:`~.RESTMain` application.

    .. attribute:: api

       Reference to the :class:`~.RESTApi` owner of this entity.

    .. attribute:: config

       Reference to the :class:`WMCore.ConfigSection` for the
       :class:`~.RESTApi` owner.

    .. attribute:: mount

       The URL mount point of the :class:`~.RESTApi` owner. Does not
       include the name of this entity.
    """

    def __init__(self, app, api, config, mount):
        self.app = app
        self.api = api
        self.config = config
        self.mount = mount


######################################################################
######################################################################
def restcall(func=None, args=None, generate="result", **kwargs):
    """Mark a method for use in REST API calls.

    This is a decorator to mark a callable, such as :class:`~.RESTEntity`
    method, an exposed REST interface. It must be applied on functions and
    methods given to :meth:`.MiniRESTApi._addAPI` and :meth:`.RESTApi._add`.
    It can be used either as bare-word or function-like decorator: both
    ``@restcall def f()`` and ``@restcall(foo=bar) def f()`` forms work.

    The `args` should be the parameter names accepted by the decorated
    function. If `args` is the default None, the possible arguments are
    extracted with :func:`inspect.getfullargspec`.

    The `generate` argument sets the label used by output formatters to
    surround the output. The default is "result", yielding for example
    JSON output ``{"result": [ ... ]}`` and XML output ``<app><result>
    ... </result></app>``

    The `kwargs` can supply any configuration variables which are known to
    other parts of the REST API implementation. The arguments eventually
    become fields in the "API object" created by :meth:`.MiniRESTApi._addAPI`,
    and accessible among other things to the validator functions. The most
    commonly used arguments include those listed below. See the rest of
    the module documentation for the details on them.

    =================== ======================================================
    Keyword             Purpose
    =================== ======================================================
    generate            Name of the response wrapper as described above.
    columns             List of column names for row-oriented data.
    expires             Response expire time in seconds.
    expires_opts        Additional "Cache-Control" options.
    formats             "Accept" formats and associated formatter objects.
    etag_limit          Amount of output to buffer for ETag calculation.
    compression         "Accept-Encoding" methods, empty disables compression.
    compression_level   ZLIB compression level for output (0 .. 9).
    compression_chunk   Approximate amount of output to compress at once.
    =================== ======================================================

    :returns: The original function suitably enriched with attributes if
      invoked as a function-like decorator, or a function which will apply
      the decoration if invoked as bare-word style decorator.
    """

    def apply_restcall_opts(func, args=args, generate=generate, kwargs=kwargs):
        if not func:
            raise ValueError("'restcall' must be applied to a function")
        if args == None:
            args = [a for a in inspect.getfullargspec(func).args if a != 'self']
        if args == None or not isinstance(args, list):
            raise ValueError("'args' must be defined")
        kwargs.update(generate=generate)
        setattr(func, 'rest.exposed', True)
        setattr(func, 'rest.args', args or [])
        setattr(func, 'rest.params', kwargs)
        return func

    return (func and apply_restcall_opts(func)) or apply_restcall_opts


def rows(cursor):
    """Utility function to convert a sequence `cursor` to a generator."""
    for row in cursor:
        yield row


def rxfilter(rx, select, cursor):
    """Utility function to convert a sequence `cursor` to a generator, but
    applying a filtering predicate to select which rows to return.

    The assumption is that `cursor` yields uniform sequence objects ("rows"),
    and the `select` operator can be invoked with ``select(row)`` for each
    retrieved row to return a string. If the value returned matches the
    regular expression `rx`, the original row is included in the generator
    output, otherwise it's skipped.

    :arg re.RegexObject rx: Regular expression to match against, or at least
     any object which supports ``rx.match(value)`` on the value returned by
     the ``select(row)`` operator.
    :arg callable select: An operator which returns the item to filter on,
     given a row of values, typically an :func:`operator.itemgetter`.
    :arg sequence cursor: Input sequence."""

    for row in cursor:
        if rx.match(select(row)):
            yield row
