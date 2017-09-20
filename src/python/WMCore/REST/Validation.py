from WMCore.REST.Error import *
import math, re
import numbers

def return_message(main_err, custom_err):
    if custom_err:
        return custom_err
    return main_err

def _arglist(argname, kwargs):
    val = kwargs.get(argname, None)
    if val == None:
        return []
    elif not isinstance(val, list):
        return [ val ]
    else:
        return val

def _check_rx(argname, val, custom_err = None):
    if not isinstance(val, basestring):
        raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))
    try:
        return re.compile(val)
    except:
        raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))

def _check_str(argname, val, rx, custom_err = None):
    """
    convert unicode to str first to support cherrypy 3.2.2
    This is not really check val is ASCII.
    """
    if isinstance(val, unicode):
        try:
            val = str(val)
        except:
            raise InvalidParameter(return_message("Invalid '%s' parameter" % argname, custom_err))
    if not isinstance(val, basestring) or not rx.match(val):
        raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))
    return val

def _check_ustr(argname, val, rx, custom_err = None):
    if isinstance(val, str):
        try:
            val = unicode(val, "utf-8")
        except:
            raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))
    if not isinstance(val, basestring) or not rx.match(val):
        raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))
    return val

def _check_num(argname, val, bare, minval, maxval, custom_err = None):
    if not isinstance(val, numbers.Integral) and (not isinstance(val, basestring) or (bare and not val.isdigit())):
        raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))
    try:
        n = int(val)
        if (minval != None and n < minval) or (maxval != None and n > maxval):
            raise InvalidParameter(return_message("Parameter '%s' value out of bounds" % argname, custom_err))
        return n
    except InvalidParameter:
        raise
    except:
        raise InvalidParameter(return_message("Invalid '%s' parameter" % argname, custom_err))

def _check_real(argname, val, special, minval, maxval, custom_err = None):
    if not isinstance(val, numbers.Number) and not isinstance(val, basestring):
        raise InvalidParameter(return_message("Incorrect '%s' parameter" % argname, custom_err))
    try:
        n = float(val)
        if not special and (math.isnan(n) or math.isinf(n)):
            raise InvalidParameter(return_message("Parameter '%s' improper value" % argname, custom_err))
        if (minval != None and n < minval) or (maxval != None and n > maxval):
            raise InvalidParameter(return_message("Parameter '%s' value out of bounds" % argname, custom_err))
        return n
    except InvalidParameter:
        raise
    except:
        raise InvalidParameter(return_message("Invalid '%s' parameter" % argname, custom_err))

def _validate_one(argname, param, safe, checker, optional, *args):
    val = param.kwargs.get(argname, None)
    if optional and val == None:
        safe.kwargs[argname] = None
    else:
        safe.kwargs[argname] = checker(argname, val, *args)
        del param.kwargs[argname]

def _validate_all(argname, param, safe, checker, *args):
    safe.kwargs[argname] = [checker(argname, v, *args) for v in _arglist(argname, param.kwargs)]
    if argname in param.kwargs:
        del param.kwargs[argname]

def validate_rx(argname, param, safe, optional = False, custom_err = None):
    """Validates that an argument is a valid regexp.

    Checks that an argument named `argname` exists in `param.kwargs`,
    and it a string which compiles into a python regular expression.
    If successful, the regexp object (not the string) is copied into
    `safe.kwargs` and the string value is removed from `param.kwargs`.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception."""
    _validate_one(argname, param, safe, _check_rx, optional, custom_err)

def validate_str(argname, param, safe, rx, optional = False, custom_err = None):
    """Validates that an argument is a string and matches a regexp.

    Checks that an argument named `argname` exists in `param.kwargs`
    and it is a string which matches regular expression `rx`. If
    successful the string is copied into `safe.kwargs` and the value
    is removed from `param.kwargs`.

    Use `validate_ustr` instead if the argument string might need to
    be converted from utf-8 into unicode first. Use this method only
    for inputs which are meant to be bare strings.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception."""
    _validate_one(argname, param, safe, _check_str, optional, rx, custom_err)

def validate_ustr(argname, param, safe, rx, optional = False, custom_err = None):
    """Validates that an argument is a string and matches a regexp,
    once converted from utf-8 into unicode.

    Checks that an argument named `argname` exists in `param.kwargs`
    and it is a string which matches regular expression `rx`. If
    successful the string is copied into `safe.kwargs` and the value
    is removed from `param.kwargs`.

    Use `validate_str` instead if the argument string should always be
    a bare string. This one automatically converts everything into
    unicode and expects input exclusively in utf-8, which may not be
    appropriate constraints for some uses.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception."""
    _validate_one(argname, param, safe, _check_ustr, optional, rx, custom_err)

def validate_num(argname, param, safe, optional = False,
                 bare = False, minval = None, maxval = None, custom_err = None):
    """Validates that an argument is a valid integer number.

    Checks that an argument named `argname` exists in `param.kwargs`,
    and it is an int or a string convertible to a valid number. If successful
    the integer value (not the string) is copied into `safe.kwargs`
    and the original int/string value is removed from `param.kwargs`.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception.

    If `bare` is True, the number is required to be a pure digit sequence if it is a string.
    Otherwise anything accepted by `int(val)` is acceted, including for
    example leading white space or sign. Note that either way arbitrarily
    large values are accepted; if you want to prevent abuse against big
    integers, use the `minval` and `maxval` thresholds described below,
    or check the length the of the string against some limit first.

    If `minval` or `maxval` are given, values less than or greater than,
    respectively, the threshold are rejected."""
    _validate_one(argname, param, safe, _check_num, optional, bare, minval, maxval, custom_err)

def validate_real(argname, param, safe, optional = False,
                  special = False, minval = None, maxval = None, custom_err = None):
    """Validates that an argument is a valid real number.

    Checks that an argument named `argname` exists in `param.kwargs`,
    and it is float number or a string convertible to a valid number. If successful
    the float value (not the string) is copied into `safe.kwargs`
    and the original float/string value is removed from `param.kwargs`.

    If `optional` is True, the argument is not required to exist in
    `param.kwargs`; None is then inserted into `safe.kwargs`. Otherwise
    a missing value raises an exception.

    Anything accepted by `float(val)` is accepted, including for example
    leading white space, sign and exponent. However NaN and +/- Inf are
    rejected unless `special` is True.

    If `minval` or `maxval` are given, values less than or greater than,
    respectively, the threshold are rejected."""
    _validate_one(argname, param, safe, _check_real, optional, special, minval, maxval, custom_err)

def validate_rxlist(argname, param, safe, custom_err = None):
    """Validates that an argument is an array of strings, each of which
    can be compiled into a python regexp object.

    Checks that an argument named `argname` is either a single string or
    an array of strings, each of which compiles into a regular expression.
    If successful the array is copied into `safe.kwargs` and the value is
    removed from `param.kwargs`. The value always becomes an array in
    `safe.kwargs`, even if no or only one argument was provided.

    Note that an array of zero length is accepted, meaning there were no
    `argname` parameters at all in `param.kwargs`."""
    _validate_all(argname, param, safe, _check_rx, custom_err)

def validate_strlist(argname, param, safe, rx, custom_err = None):
    """Validates that an argument is an array of strings, each of which
    matches a regexp.

    Checks that an argument named `argname` is either a single string or
    an array of strings, each of which matches the regular expression
    `rx`. If successful the array is copied into `safe.kwargs` and the
    value is removed from `param.kwargs`. The value always becomes an
    array in `safe.kwargs`, even if no or only one argument was provided.

    Use `validate_ustrlist` instead if the argument string might need
    to be converted from utf-8 into unicode first. Use this method only
    for inputs which are meant to be bare strings.

    Note that an array of zero length is accepted, meaning there were no
    `argname` parameters at all in `param.kwargs`."""
    _validate_all(argname, param, safe, _check_str, rx, custom_err)

def validate_ustrlist(argname, param, safe, rx, custom_err = None):
    """Validates that an argument is an array of strings, each of which
    matches a regexp once converted from utf-8 into unicode.

    Checks that an argument named `argname` is either a single string or
    an array of strings, each of which matches the regular expression
    `rx`. If successful the array is copied into `safe.kwargs` and the
    value is removed from `param.kwargs`. The value always becomes an
    array in `safe.kwargs`, even if no or only one argument was provided.

    Use `validate_strlist` instead if the argument strings should always
    be bare strings. This one automatically converts everything into
    unicode and expects input exclusively in utf-8, which may not be
    appropriate constraints for some uses.

    Note that an array of zero length is accepted, meaning there were no
    `argname` parameters at all in `param.kwargs`."""
    _validate_all(argname, param, safe, _check_ustr, rx, custom_err)

def validate_numlist(argname, param, safe, bare=False, minval=None, maxval=None, custom_err = None):
    """Validates that an argument is an array of integers, as checked by
    `validate_num()`.

    Checks that an argument named `argname` is either a single string/int or
    an array of strings/int, each of which validates with `validate_num` and
    `bare`, `minval` and `maxval` arguments.  If successful the array is
    copied into `safe.kwargs` and the value is removed from `param.kwargs`.
    The value always becomes an array in `kwsafe`, even if no or only one
    argument was provided.

    Note that an array of zero length is accepted, meaning there were no
    `argname` parameters at all in `param.kwargs`."""
    _validate_all(argname, param, safe, _check_num, bare, minval, maxval, custom_err)

def validate_reallist(argname, param, safe, special=False, minval=None, maxval=None, custom_err = None):
    """Validates that an argument is an array of integers, as checked by
    `validate_real()`.

    Checks that an argument named `argname` is either a single string/float or
    an array of strings/floats, each of which validates with `validate_real` and
    `special`, `minval` and `maxval` arguments.  If successful the array is
    copied into `safe.kwargs` and the value is removed from `param.kwargs`.
    The value always becomes an array in `safe.kwargs`, even if no or only
    one argument was provided.

    Note that an array of zero length is accepted, meaning there were no
    `argname` parameters at all in `param.kwargs`."""
    _validate_all(argname, param, safe, _check_real, special, minval, maxval, custom_err)

def validate_no_more_input(param):
    """Verifies no more input is left in `param.args` or `param.kwargs`."""
    if param.args:
        raise InvalidParameter("Excess path arguments, not validated args='%s'" % param.args)
    if param.kwargs:
        raise InvalidParameter("Excess keyword arguments, not validated kwargs='%s'" % param.kwargs)

def validate_lengths(safe, *names):
    """Verifies that all `names` exist in `safe.kwargs`, are lists, and
    all the lists have the same length. This is convenience function for
    checking that an API accepting multiple values receives equal number
    of values for all of its parameters."""
    refname = names[0]
    if refname not in safe.kwargs or not isinstance(safe.kwargs[refname], list):
        raise InvalidParameter("Incorrect '%s' parameter" % refname)

    reflen = len(safe.kwargs[refname])
    for other in names[1:]:
        if other not in safe.kwargs or not isinstance(safe.kwargs[other], list):
            raise InvalidParameter("Incorrect '%s' parameter" % other)
        elif len(safe.kwargs[other]) != reflen:
            raise InvalidParameter("Mismatched number of arguments: %d %s vs. %d %s"
                                   % (reflen, refname, len(safe.kwargs[other]), other))
