from __future__ import print_function

import gzip
from builtins import str, bytes, object

from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytes, encodeUnicodeToBytesConditional
from future.utils import viewitems

import hashlib
import json
import xml.sax.saxutils
import zlib
from traceback import format_exc

import cherrypy

from WMCore.REST.Error import RESTError, ExecutionError, report_rest_error

try:
    from cherrypy.lib import httputil
except ImportError:
    from cherrypy.lib import http as httputil

def vary_by(header):
    """Add 'Vary' header for `header`."""
    varies = cherrypy.response.headers.get('Vary', '')
    varies = [x.strip() for x in varies.split(",") if x.strip()]
    if header not in varies:
        varies.append(header)
    cherrypy.response.headers['Vary'] = ", ".join(varies)

def is_iterable(obj):
    """Check if `obj` is iterable."""
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True

class RESTFormat(object):
    def __call__(self, stream, etag):
        """Main entry point for generating output for `stream` using `etag`
        object to generate ETag header. Returns a generator function for
        producing a verbatim copy of `stream` item, including any premables
        and trailers needed for the selected format. The intention is that
        the caller will use the iterable to generate chunked HTTP transfer
        encoding, or a simple result such as an image."""
        # Make 'stream' iterable. We convert everything to chunks here.
        # The final stream consumer will collapse small responses back
        # to a single string. Convert files to 1MB chunks.
        if stream is None:
            stream = ['']
        elif isinstance(stream, (str, bytes)):
            stream = [stream]
        elif hasattr(stream, "read"):
            # types.FileType is not available anymore in python3,
            # using it raises pylint W1624.
            # Since cherrypy.lib.file_generator only uses the .read() attribute
            # of a file, we simply check if stream.read() is present instead.
            # https://github.com/cherrypy/cherrypy/blob/2a8aaccd649eb1011382c39f5cd93f76f980c0b1/cherrypy/lib/__init__.py#L64
            stream = cherrypy.lib.file_generator(stream, 512 * 1024)

        return self.stream_chunked(stream, etag, *self.chunk_args(stream))

    def chunk_args(self, stream):
        """Return extra arguments needed for `stream_chunked()`. The default
        return an empty tuple, so no extra arguments. Override in the derived
        class if `stream_chunked()` needs preamble or trailer arguments."""
        return tuple()

class XMLFormat(RESTFormat):
    """Format an iterable of objects into XML encoded in UTF-8.

    Generates normally first a preamble, a stream of XML-rendered objects,
    then the trailer, computing an ETag on the output string in the process.
    This is designed exclusively for use with iterables for chunked transfer
    encoding HTTP responses; it's not a general purpose formatting utility.

    Outputs first a preamble, then XML encoded output of input stream, and
    finally a trailer. Any exceptions raised by input stream are reported to
    `report_rest_error` and swallowed, as this is normally used to generate
    output for CherryPy responses, which cannot handle exceptions reasonably
    after the output generation begins; later processing may reconvert those
    back to exceptions however (cf. stream_maybe_etag()). Once the preamble
    has been emitted, the trailer is also emitted even if the input stream
    raises an exception, in order to make the output well-formed; the client
    must inspect the X-REST-Status trailer header to find out if it got the
    complete output. No ETag header is generated in case of an exception.

    The ETag generation is deterministic only if iterating over input is
    deterministic. Beware in particular the key order for a dict is
    arbitrary and may differ for two semantically identical dicts.

    A X-REST-Status trailer header is added only in case of error. There is
    normally 'X-REST-Status: 100' in normal response headers, and it remains
    valid in case of success.

    The output is generated as an XML document whose top-level entity name
    is defined by the label given at the formatter construction time. The
    caller must define ``cherrypy.request.rest_generate_data`` to element
    name for wrapping stream contents. Usually the top-level entity is the
    application name and the ``cherrypy.request.rest_generate_data`` is
    ``result``.

    Iterables are output as ``<array><i>ITEM</i><i>ITEM</i></array>``,
    dictionaries as ``<dict><key>KEY</key><value>VALUE</value></dict>``.
    `None` is output as empty contents, and hence there is no way to
    distinguish `None` and an empty string from each other. Scalar types
    are output as rendered by `str()`, but obviously XML encoding unsafe
    characters. This class does not support formatting arbitrary types.

    The formatter does not insert any spaces into the output. Although the
    output is generated as a preamble, stream of objects, and trailer just
    like by the `JSONFormatter`, each of which is a separate HTTP transfer
    chunk, the output does *not* have guaranteed line-oriented structure
    like the `JSONFormatter` produces. Note in particular that if the data
    stream contains strings with newlines, the output will have arbitrary
    line structure. On the other hand, as the output is well-formed XML,
    virtually all SAX processors can read the stream incrementally even if
    the client isn't able to fully preserve chunked HTTP transfer encoding."""

    def __init__(self, label):
        self.label = label

    @staticmethod
    def format_obj(obj):
        """Render an object `obj` into XML."""
        if isinstance(obj, type(None)):
            result = ""
        elif isinstance(obj, str):
            result = xml.sax.saxutils.escape(obj).encode("utf-8")
        elif isinstance(obj, bytes):
            result = xml.sax.saxutils.escape(obj)
        elif isinstance(obj, (int, float, bool)):
            result = xml.sax.saxutils.escape(str(obj)).encode("utf-8")
        elif isinstance(obj, dict):
            result = "<dict>"
            for k, v in viewitems(obj):
                result += "<key>%s</key><value>%s</value>" % \
                  (xml.sax.saxutils.escape(k).encode("utf-8"),
                   XMLFormat.format_obj(v))
            result += "</dict>"
        elif is_iterable(obj):
            result = "<array>"
            for v in obj:
                result += "<i>%s</i>" % XMLFormat.format_obj(v)
            result += "</array>"
        else:
            cherrypy.log("cannot represent object of type %s in xml (%s)"
                         % (type(obj).__class__.__name__, repr(obj)))
            raise ExecutionError("cannot represent object in xml")
        return result

    def stream_chunked(self, stream, etag, preamble, trailer):
        """Generator for actually producing the output."""
        try:
            etag.update(preamble)
            yield preamble

            try:
                for obj in stream:
                    chunk = XMLFormat.format_obj(obj)
                    etag.update(chunk)
                    yield chunk
            except GeneratorExit:
                etag.invalidate()
                trailer = None
                raise
            finally:
                if trailer:
                    etag.update(trailer)
                    yield trailer

        except RESTError as e:
            etag.invalidate()
            report_rest_error(e, format_exc(), False)
        except Exception as e:
            etag.invalidate()
            report_rest_error(ExecutionError(), format_exc(), False)

    def chunk_args(self, stream):
        """Return header and trailer needed to wrap `stream` as XML reply."""
        preamble = "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>\n"
        preamble += "<%s>" % self.label
        if cherrypy.request.rest_generate_preamble:
            desc = self.format_obj(cherrypy.request.rest_generate_preamble)
            preamble += "<desc>%s</desc>" % desc
        preamble += "<%s>" % cherrypy.request.rest_generate_data
        trailer = "</%s></%s>" % (cherrypy.request.rest_generate_data, self.label)
        return preamble, trailer

class JSONFormat(RESTFormat):
    """Format an iterable of objects into JSON.

    Generates normally first a preamble, a stream of JSON-rendered objects,
    then the trailer, computing an ETag on the output string in the process.
    This is designed exclusively for use with iterables for chunked transfer
    encoding HTTP responses; it's not a general purpose formatting utility.

    Outputs first a preamble, then JSON encoded output of input stream, and
    finally a trailer. Any exceptions raised by input stream are reported to
    `report_rest_error` and swallowed, as this is normally used to generate
    output for CherryPy responses, which cannot handle exceptions reasonably
    after the output generation begins; later processing may reconvert those
    back to exceptions however (cf. stream_maybe_etag()). Once the preamble
    has been emitted, the trailer is also emitted even if the input stream
    raises an exception, in order to make the output well-formed; the client
    must inspect the X-REST-Status trailer header to find out if it got the
    complete output. No ETag header is generated in case of an exception.

    The ETag generation is deterministic only if `cjson.encode()` output is
    deterministic for the input. Beware in particular the key order for a
    dict is arbitrary and may differ for two semantically identical dicts.

    A X-REST-Status trailer header is added only in case of error. There is
    normally 'X-REST-Status: 100' in normal response headers, and it remains
    valid in case of success.

    The output is always generated as a JSON dictionary. The caller must
    define ``cherrypy.request.rest_generate_data`` as the key for actual
    contents, usually something like "result". The `stream` value will be
    generated as an array value for that key.

    If ``cherrypy.request.rest_generate_preamble`` is a non-empty list, it
    is output as the ``desc`` key value in the preamble before outputting
    the `stream` contents. Otherwise the output consists solely of `stream`.
    A common use of ``rest_generate_preamble`` is list of column labels
    with `stream` an iterable of lists of column values.

    The output is guaranteed to contain one line of preamble which starts a
    dictionary and an array ("``{key: [``"), one line of JSON rendering of
    each object in `stream`, with the first line starting with exactly one
    space and second and subsequent lines starting with a comma, and one
    final trailer line consisting of "``]}``". Each line is generated as a
    HTTP transfer chunk. This format is fixed so readers can be constructed
    to read and parse the stream incrementally one line at a time,
    facilitating maximum throughput processing of the response."""

    def stream_chunked(self, stream, etag, preamble, trailer):
        """Generator for actually producing the output."""
        comma = " "

        try:
            if preamble:
                etag.update(preamble)
                yield preamble

            try:
                for obj in stream:
                    chunk = comma + json.dumps(obj) + "\n"
                    etag.update(chunk)
                    yield chunk
                    comma = ","
            except GeneratorExit:
                etag.invalidate()
                trailer = None
                raise
            except Exception as exp:
                print("ERROR, json.dumps failed to serialize %s, type %s\nException: %s" \
                        % (obj, type(obj), str(exp)))
                raise
            finally:
                if trailer:
                    etag.update(trailer)
                    yield trailer

            cherrypy.response.headers["X-REST-Status"] = 100
        except RESTError as e:
            etag.invalidate()
            report_rest_error(e, format_exc(), False)
        except Exception as e:
            etag.invalidate()
            report_rest_error(ExecutionError(), format_exc(), False)

    def chunk_args(self, stream):
        """Return header and trailer needed to wrap `stream` as JSON reply."""
        comma = ""
        preamble = "{"
        trailer = "]}\n"
        if cherrypy.request.rest_generate_preamble:
            desc = json.dumps(cherrypy.request.rest_generate_preamble)
            preamble += '"desc": %s' % desc
            comma = ", "
        preamble += '%s"%s": [\n' % (comma, cherrypy.request.rest_generate_data)
        return preamble, trailer

class PrettyJSONFormat(JSONFormat):
    """ Format used for human, (web browser)"""

    def stream_chunked(self, stream, etag, preamble, trailer):
        """Generator for actually producing the output."""
        comma = " "

        try:
            if preamble:
                etag.update(preamble)
                yield preamble

            try:
                for obj in stream:
                    chunk = comma + json.dumps(obj, indent=2)
                    etag.update(chunk)
                    yield chunk
                    comma = ","
            except GeneratorExit:
                etag.invalidate()
                trailer = None
                raise
            finally:
                if trailer:
                    etag.update(trailer)
                    yield trailer

            cherrypy.response.headers["X-REST-Status"] = 100
        except RESTError as e:
            etag.invalidate()
            report_rest_error(e, format_exc(), False)
        except Exception as e:
            etag.invalidate()
            report_rest_error(ExecutionError(), format_exc(), False)

class PrettyJSONHTMLFormat(PrettyJSONFormat):
    """ Format used for human, (web browser) wrap around html tag on json"""

    @staticmethod
    def format_obj(obj):
        """Render an object `obj` into HTML."""
        if isinstance(obj, type(None)):
            result = ""
        elif isinstance(obj, str):
            obj = xml.sax.saxutils.quoteattr(obj)
            result = "<pre>%s</pre>" % obj if '\n' in obj else obj
        elif isinstance(obj, bytes):
            obj = xml.sax.saxutils.quoteattr(str(obj, "utf-8"))
            result = "<pre>%s</pre>" % obj if '\n' in obj else obj
        elif isinstance(obj, (int, float, bool)):
            result = "%s" % obj
        elif isinstance(obj, dict):
            result = "<ul>"
            for k, v in viewitems(obj):
                result += "<li><b>%s</b>: %s</li>" % (k, PrettyJSONHTMLFormat.format_obj(v))
            result += "</ul>"
        elif is_iterable(obj):
            empty = True
            result = "<details open><ul>"
            for v in obj:
                empty = False
                result += "<li>%s</li>" % PrettyJSONHTMLFormat.format_obj(v)
            result += "</ul></details>"
            if empty:
                result = ""
        else:
            cherrypy.log("cannot represent object of type %s in xml (%s)"
                         % (type(obj).__class__.__name__, repr(obj)))
            raise ExecutionError("cannot represent object in xml")
        return result

    def stream_chunked(self, stream, etag, preamble, trailer):
        """Generator for actually producing the output."""
        try:
            etag.update(preamble)
            yield preamble

            try:
                for obj in stream:
                    chunk = PrettyJSONHTMLFormat.format_obj(obj)
                    etag.update(chunk)
                    yield chunk
            except GeneratorExit:
                etag.invalidate()
                trailer = None
                raise
            finally:
                if trailer:
                    etag.update(trailer)
                    yield trailer

        except RESTError as e:
            etag.invalidate()
            report_rest_error(e, format_exc(), False)
        except Exception as e:
            etag.invalidate()
            report_rest_error(ExecutionError(), format_exc(), False)

    def chunk_args(self, stream):
        """Return header and trailer needed to wrap `stream` as XML reply."""
        preamble = "<html><body>"
        trailer = "</body></html>"
        return preamble, trailer

class RawFormat(RESTFormat):
    """Format an iterable of objects as raw data.

    Generates raw data completely unmodified, for example image data or
    streaming arbitrary external data files including even plain text.
    Computes an ETag on the output in the process. The result is always
    chunked, even simple strings on input. Usually small enough responses
    will automatically be converted back to a single string response post
    compression and ETag processing.

    Any exceptions raised by input stream are reported to `report_rest_error`
    and swallowed, as this is normally used to generate output for CherryPy
    responses, which cannot handle exceptions reasonably after the output
    generation begins; later processing may reconvert those back to exceptions
    however (cf. stream_maybe_etag()). A X-REST-Status trailer header is added
    if (and only if) an exception occurs; the client must inspect that to find
    out if it got the complete output. There is normally 'X-REST-Status: 100'
    in normal response headers, and it remains valid in case of success.
    No ETag header is generated in case of an exception."""

    def stream_chunked(self, stream, etag):
        """Generator for actually producing the output."""
        try:
            for chunk in stream:
                etag.update(chunk)
                yield chunk

        except RESTError as e:
            etag.invalidate()
            report_rest_error(e, format_exc(), False)
        except Exception as e:
            etag.invalidate()
            report_rest_error(ExecutionError(), format_exc(), False)
        except BaseException:
            etag.invalidate()
            raise

class DigestETag(object):
    """Compute hash digest over contents for ETag header."""
    algorithm = None

    def __init__(self, algorithm=None):
        """Prepare ETag computer."""
        self.digest = hashlib.new(algorithm or self.algorithm)

    def update(self, val):
        """Process response data `val`."""
        if self.digest:
            self.digest.update(encodeUnicodeToBytes(val))

    def value(self):
        """Return ETag header value for current input."""
        return self.digest and '"%s"' % self.digest.hexdigest()

    def invalidate(self):
        """Invalidate the ETag calculator so value() will return None."""
        self.digest = None

class MD5ETag(DigestETag):
    """Compute MD5 hash over contents for ETag header."""
    algorithm = 'md5'

class SHA1ETag(DigestETag):
    """Compute SHA1 hash over contents for ETag header."""
    algorithm = 'sha1'

def _stream_compress_identity(reply, *args):
    """Streaming compressor which returns original data unchanged."""
    return reply

def _stream_compress_deflate(reply, compress_level, max_chunk):
    """Streaming compressor for the 'deflate' method. Generates output that
    is guaranteed to expand at the exact same chunk boundaries as original
    reply stream."""

    # Create zlib compression object, with raw data stream (negative window size)
    z = zlib.compressobj(compress_level, zlib.DEFLATED, -zlib.MAX_WBITS,
                         zlib.DEF_MEM_LEVEL, 0)

    # Data pending compression. We only take entire chunks from original
    # reply. Then process reply one chunk at a time. Whenever we have enough
    # data to compress, spit it out flushing the zlib engine entirely, so we
    # respect original chunk boundaries.
    npending = 0
    pending = []
    for chunk in reply:
        pending.append(chunk)
        npending += len(chunk)
        if npending >= max_chunk:
            part = z.compress(encodeUnicodeToBytes("".join(pending))) + z.flush(zlib.Z_FULL_FLUSH)
            pending = []
            npending = 0
            yield part

    # Crank the compressor one more time for remaining output.
    if npending:
        yield z.compress(encodeUnicodeToBytes("".join(pending))) + z.flush(zlib.Z_FINISH)


def _stream_compress_gzip(reply, compress_level, *args):
    """Streaming compressor for the 'gzip' method. Generates output that
    is guaranteed to expand at the exact same chunk boundaries as original
    reply stream."""
    data = []
    for chunk in reply:
        data.append(chunk)
    if data:
        yield gzip.compress(encodeUnicodeToBytes("".join(data)), compress_level)


# : Stream compression methods.
_stream_compressor = {
  'identity': _stream_compress_identity,
  'deflate': _stream_compress_deflate,
  'gzip': _stream_compress_gzip
}

def stream_compress(reply, available, compress_level, max_chunk):
    """If compression has been requested via Accept-Encoding request header,
    and is granted for this response via `available` compression methods,
    convert the streaming `reply` into another streaming response which is
    compressed at the exact chunk boundaries of the original response,
    except that individual chunks may be coalesced up to `max_chunk` size.
    The `compression_level` tells how hard to compress, zero disables the
    compression entirely."""

    global _stream_compressor
    for enc in cherrypy.request.headers.elements('Accept-Encoding'):
        if enc.value not in available:
            continue

        elif enc.value in _stream_compressor and compress_level > 0:
            # Add 'Vary' header for 'Accept-Encoding'.
            vary_by('Accept-Encoding')

            # Compress contents at original chunk boundaries.
            if 'Content-Length' in cherrypy.response.headers:
                del cherrypy.response.headers['Content-Length']
            cherrypy.response.headers['Content-Encoding'] = enc.value
            return _stream_compressor[enc.value](reply, compress_level, max_chunk)

    return reply

def _etag_match(status, etagval, match, nomatch):
    """Match ETag value against any If-Match / If-None-Match headers."""
    # Execute conditions only for status 2xx. We only handle GET/HEAD
    # requests here, it makes no sense to try to do this for PUT etc.
    # as they need to be handled as request pre-condition, not in the
    # streaming out part here.
    if cherrypy.request.method in ('GET', 'HEAD'):
        status, dummyReason, dummyMsg = httputil.valid_status(status)
        if status >= 200 and status <= 299:
            if match and ("*" in match or etagval in match):
                raise cherrypy.HTTPError(412, "Precondition on ETag %s failed" % etagval)
            if nomatch and ("*" in nomatch or etagval in nomatch):
                raise cherrypy.HTTPRedirect([], 304)

def _etag_tail(head, tail, etag):
    """Generator which first returns anything in `head`, then `tail`.
    Sets ETag header at the end to value of `etag` if it's defined and
    yields a value."""
    for chunk in head:
        yield encodeUnicodeToBytes(chunk)

    for chunk in tail:
        yield encodeUnicodeToBytes(chunk)

    etagval = (etag and etag.value())
    if etagval:
        cherrypy.response.headers["ETag"] = etagval

def stream_maybe_etag(size_limit, etag, reply):
    """Maybe generate ETag header for the response, and handle If-Match
    and If-None-Match request headers. Consumes the reply until at most
    `size_limit` bytes. If the response fits into that size, adds the
    ETag header and matches it against any If-Match / If-None-Match
    request headers and replies appropriately.

    If the response is fully buffered, and the `reply` generator actually
    results in an error and sets X-Error-HTTP / X-Error-Detail headers,
    converts that error back into a real HTTP error response. Otherwise
    responds with the fully buffered body directly, without generator
    and chunking. In other words, responses smaller than `size_limit`
    are always fully buffered and replied immediately without chunking.
    If the response is not fully buffered, it's guaranteed to be output
    at original chunk boundaries.

    Note that if this function is fed the output from `stream_compress()`
    as it normally would be, the `size_limit` constrains the compressed
    size, and chunk boundaries correspond to compressed chunks."""

    req = cherrypy.request
    res = cherrypy.response
    match = [str(x) for x in (req.headers.elements('If-Match') or [])]
    nomatch = [str(x) for x in (req.headers.elements('If-None-Match') or [])]

    # If ETag is already set, match conditions and output without buffering.
    etagval = res.headers.get('ETag', None)
    if etagval:
        _etag_match(res.status or 200, etagval, match, nomatch)
        res.headers['Trailer'] = 'X-REST-Status'
        return _etag_tail([], reply, None)

    # Buffer up to size_limit bytes internally. This interally builds up the
    # ETag value inside 'etag'. In case of exceptions the ETag invalidates.
    # If we exceed the limit, fall back to streaming without checking ETag
    # against If-Match/If-None-Match. We'll still set the ETag in the trailer
    # headers, so clients which understand trailers will get the value; most
    # clients including browsers will ignore them.
    size = 0
    result = []
    for chunk in reply:
        result.append(chunk)
        size += len(chunk)
        if size > size_limit:
            res.headers['Trailer'] = 'X-REST-Status'
            return _etag_tail(result, reply, etag)

    # We've buffered the entire response, but it may be an error reply. The
    # generator code does not know if it's allowed to raise exceptions, so
    # it swallows all errors and converts them into X-* headers. We recover
    # the original HTTP response code and message from X-Error-{HTTP,Detail}
    # headers, if any are present.
    err = res.headers.get('X-Error-HTTP', None)
    if err:
        message = res.headers.get('X-Error-Detail', 'Original error lost')
        raise cherrypy.HTTPError(int(err), message)

    # OK, we buffered the entire reply and it's ok. Check ETag match criteria.
    # The original stream generator must guarantee that if it fails it resets
    # the 'etag' value, even if the error handlers above didn't run.
    etagval = etag.value()
    if etagval:
        res.headers['ETag'] = etagval
        _etag_match(res.status or 200, etagval, match, nomatch)

    # OK, respond with the buffered reply as a plain string.
    res.headers['Content-Length'] = size
    # TODO investigate why `result` is a list of bytes strings in py3
    # The current solution seems to work in both py2 and py3
    resp = b"" if PY3 else ""
    for item in result:
        resp += encodeUnicodeToBytesConditional(item, condition=PY3)
    assert len(resp) == size
    return resp
