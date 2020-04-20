import random, cherrypy

class RESTError(Exception):
    """Base class for REST errors.

    .. attribute:: http_code

       Integer, HTTP status code for this error. Also emitted as X-Error-HTTP
       header value.

    .. attribute:: app_code

       Integer, application error code, to be emitted as X-REST-Status header.

    .. attribute:: message

       String, information about the error, to be emitted as X-Error-Detail
       header. Should not contain anything sensitive, and in particular should
       never include any unvalidated or unsafe data, e.g. input parameters or
       data from a database. Normally a fixed label with one-to-one match with
       the :obj:`app-code`. If the text exceeds 200 characters, it's truncated.
       Since this is emitted as a HTTP header, it cannot contain newlines or
       anything encoding-dependent.

    .. attribute:: info

       String, additional information beyond :obj:`message`, to be emitted as
       X-Error-Info header. Like :obj:`message` should not contain anything
       sensitive or unsafe, or text inappropriate for a HTTP response header,
       and should be short enough to fit in 200 characters. This is normally
       free form text to clarify why the error happened.

    .. attribute:: errid

       String, random unique identifier for this error, to be emitted as
       X-Error-ID header and output into server logs when logging the error.
       The purpose is that clients save this id when they receive an error,
       and further error reporting or debugging can use this value to identify
       the specific error, and for example to grep logs for more information.

    .. attribute:: errobj

       If the problem was caused by another exception being raised in the code,
       reference to the original exception object. For example if the code dies
       with an :class:`KeyError`, this is the original exception object. This
       error is logged to the server logs when reporting the error, but no
       information about it is returned to the HTTP client.

    .. attribute:: trace

       The origin of the exception as returned by :func:`format_exc`. The full
       trace is emitted to the server logs, each line prefixed with timestamp.
       This information is not returned to the HTTP client.
    """

    http_code = None
    app_code = None
    message = None
    info = None
    errid = None
    errobj = None
    trace = None

    def __init__(self, info = None, errobj = None, trace = None):
        self.errid = "%032x" % random.randrange(1 << 128)
        self.errobj = errobj
        self.info = info
        self.trace = trace

    def __str__(self):
        return "%s %s [HTTP %d, APP %d, MSG %s, INFO %s, ERR %s]" \
          % (self.__class__.__name__, self.errid, self.http_code, self.app_code,
             repr(self.message).replace("\n", " ~~ "),
             repr(self.info).replace("\n", " ~~ "),
             repr(self.errobj).replace("\n", " ~~ "))

class NotAcceptable(RESTError):
    "Client did not specify format it accepts, or no compatible format was found."
    http_code = 406
    app_code = 201
    message = "Not acceptable"

class UnsupportedMethod(RESTError):
    "Client used HTTP request method which isn't supported for any API call."
    http_code = 405
    app_code = 202
    message = "Request method not supported"

class MethodWithoutQueryString(RESTError):
    "Client provided a query string which isn't acceptable for this request method."
    http_code = 405
    app_code = 203
    message = "Query arguments not supported for this request method"

class APIMethodMismatch(RESTError):
    """Both the API and HTTP request methods are supported, but not in that
    combination."""
    http_code = 405
    app_code = 204
    message = "API not supported for this request method"

class APINotSpecified(RESTError):
    "The request URL is missing API argument."
    http_code = 400
    app_code = 205
    message = "API not specified"

class NoSuchInstance(RESTError):
    """The request URL is missing instance argument or the specified instance
    does not exist."""
    http_code = 404
    app_code = 206
    message = "No such instance"

class APINotSupported(RESTError):
    "The request URL provides wrong API argument."
    http_code = 404
    app_code = 207
    message = "API not supported"

class DataCacheEmpty(RESTError):
    "The wmstats data cache has not be created."
    http_code = 503
    app_code = 208
    message = "DataCache is Empty"

class DatabaseError(RESTError):
    """Parent class for database-related errors.

    .. attribute: lastsql

       A tuple of *(sql, binds, kwbinds),* where `sql` is the last SQL statement
       executed and `binds`, `kwbinds` are the bind values used with it. Any
       sensitive parts like passwords have already been censored from the `sql`
       string. Note that for massive requests `binds` or `kwbinds` can get large.
       These are logged out in the server logs when reporting the error, but no
       information about these are returned to the HTTP client.

    .. attribute: intance

       String, the database instance for which the error occurred. This is
       reported in the error message output to server logs, but no information
       about this is returned to the HTTP client."""

    lastsql = None
    instance = None
    def __init__(self, info = None, errobj = None, trace = None,
                 lastsql = None, instance = None):
        RESTError.__init__(self, info, errobj, trace)
        self.lastsql = lastsql
        self.instance = instance

class DatabaseUnavailable(DatabaseError):
    """The instance argument is correct, but cannot connect to the database.
    This error will only occur at initial attempt to connect to the database,
    :class:`~.DatabaseConnectionError` is raised instead if the connection
    ends prematurely after the transaction has already begun successfully."""
    http_code = 503
    app_code = 401
    message = "Database unavailable"

class DatabaseConnectionError(DatabaseError):
    """Database was available when the operation started, but the connection
    was lost or otherwise failed during the application operation."""
    http_code = 504
    app_code = 402
    message = "Database connection failure"

class DatabaseExecutionError(DatabaseError):
    """Database operation failed."""
    http_code = 500
    app_code = 403
    message = "Execution error"

class MissingParameter(RESTError):
    "Client did not supply a parameter which is required."
    http_code = 400
    app_code = 301
    message = "Missing required parameter"

class InvalidParameter(RESTError):
    "Client supplied invalid value for a parameter."
    http_code = 400
    app_code = 302
    message = "Invalid input parameter"

class MissingObject(RESTError):
    """An object required for the operation is missing. This might be a
    pre-requisite needed to create a reference, or attempt to delete
    an object which does not exist."""
    http_code = 400
    app_code = 303
    message = "Required object is missing"

class TooManyObjects(RESTError):
    """Too many objects matched specified criteria. Usually this means
    more than one object was matched, deleted, or inserted, when only
    exactly one should have been subject to the operation."""
    http_code = 400
    app_code = 304
    message = "Too many objects"

class ObjectAlreadyExists(RESTError):
    """An already existing object is on the way of the operation. This
    is usually caused by uniqueness constraint violations when creating
    new objects."""
    http_code = 400
    app_code = 305
    message = "Object already exists"

class InvalidObject(RESTError):
    "The specified object is invalid."
    http_code = 400
    app_code = 306
    message = "Invalid object"

class ExecutionError(RESTError):
    """Input was in principle correct but there was an error processing
    the request. This normally means either programming error, timeout, or
    an unusual and unexpected problem with the database. For security reasons
    little additional information is returned. If the problem persists, client
    should contact service operators. The returned error id can be used as a
    reference."""
    http_code = 500
    app_code = 403
    message = "Execution error"

def report_error_header(header, val):
    """If `val` is non-empty, set CherryPy response `header` to `val`.
    Replaces all newlines with "; " characters. If the resulting value is
    longer than 200 characters, truncates it to the first 197 characters
    and leaves a trailing ellipsis "..."."""
    if val:
        val = val.replace("\n", "; ")
        if len(val) > 200: val = val[:197] + "..."
        cherrypy.response.headers[header] = val

def report_rest_error(err, trace, throw):
    """Report a REST error: generate an appropriate log message, set the
    response headers and raise an appropriate :class:`~.HTTPError`.

    Normally `throw` would be True to translate the exception `err` into
    a HTTP server error, but the function can also be called with `throw`
    set to False if the purpose is merely to log an exception message.

    :arg err: exception object.
    :arg trace: stack trace to use in case `err` doesn't have one.
    :arg throw: raise a :class:`~.HTTPError` if True."""
    if isinstance(err, DatabaseError) and err.errobj:
        offset = None
        sql, binds, kwbinds = err.lastsql
        if sql and err.errobj.args and hasattr(err.errobj.args[0], 'offset'):
            offset = err.errobj.args[0].offset
            sql = sql[:offset] + "<**>" + sql[offset:]
        cherrypy.log("SERVER DATABASE ERROR %d/%d %s %s.%s %s [instance: %s] (%s);"
                     " last statement: %s; binds: %s, %s; offset: %s"
                     % (err.http_code, err.app_code, err.message,
                        getattr(err.errobj, "__module__", "__builtins__"),
                        err.errobj.__class__.__name__,
                        err.errid, err.instance, str(err.errobj).rstrip(),
                        sql, binds, kwbinds, offset))
        for line in err.trace.rstrip().split("\n"): cherrypy.log("  " + line)
        cherrypy.response.headers["X-REST-Status"] = str(err.app_code)
        cherrypy.response.headers["X-Error-HTTP"] = str(err.http_code)
        cherrypy.response.headers["X-Error-ID"] = err.errid
        report_error_header("X-Error-Detail", err.message)
        report_error_header("X-Error-Info", err.info)
        if throw: raise cherrypy.HTTPError(err.http_code, err.message)
    elif isinstance(err, RESTError):
        if err.errobj:
            cherrypy.log("SERVER REST ERROR %s.%s %s (%s); derived from %s.%s (%s)"
                         % (err.__module__, err.__class__.__name__,
                            err.errid, err.message,
                            getattr(err.errobj, "__module__", "__builtins__"),
                            err.errobj.__class__.__name__,
                            str(err.errobj).rstrip()))
            trace = err.trace
        else:
            cherrypy.log("SERVER REST ERROR %s.%s %s (%s)"
                         % (err.__module__, err.__class__.__name__,
                            err.errid, err.message))
        for line in trace.rstrip().split("\n"): cherrypy.log("  " + line)
        cherrypy.response.headers["X-REST-Status"] = str(err.app_code)
        cherrypy.response.headers["X-Error-HTTP"] = str(err.http_code)
        cherrypy.response.headers["X-Error-ID"] = err.errid
        report_error_header("X-Error-Detail", err.message)
        report_error_header("X-Error-Info", err.info)
        if throw: raise cherrypy.HTTPError(err.http_code, err.message)
    elif isinstance(err, cherrypy.HTTPError):
        errid = "%032x" % random.randrange(1 << 128)
        cherrypy.log("SERVER HTTP ERROR %s.%s %s (%s)"
                     % (err.__module__, err.__class__.__name__,
                        errid, str(err).rstrip()))
        for line in trace.rstrip().split("\n"): cherrypy.log("  " + line)
        cherrypy.response.headers["X-REST-Status"] = str(200)
        cherrypy.response.headers["X-Error-HTTP"] = str(err.status)
        cherrypy.response.headers["X-Error-ID"] = errid
        report_error_header("X-Error-Detail", err._message)
        if throw: raise err
    else:
        errid = "%032x" % random.randrange(1 << 128)
        cherrypy.log("SERVER OTHER ERROR %s.%s %s (%s)"
                     % (getattr(err, "__module__", "__builtins__"),
                        err.__class__.__name__,
                        errid, str(err).rstrip()))
        for line in trace.rstrip().split("\n"): cherrypy.log("  " + line)
        cherrypy.response.headers["X-REST-Status"] = 400
        cherrypy.response.headers["X-Error-HTTP"] = 500
        cherrypy.response.headers["X-Error-ID"] = errid
        report_error_header("X-Error-Detail", "Server error")
        if throw: raise cherrypy.HTTPError(500, "Server error")
