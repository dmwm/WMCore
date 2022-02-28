#!/usr/bin/env python
"""
File       : TwPrint.py

Description:

A simple textwrap based printer for nested dictionaries.

"""
from textwrap import TextWrapper
from collections import OrderedDict


def twClosure(replace_whitespace=False,
              break_long_words=False,
              maxWidth=120,
              maxLength=-1,
              maxDepth=-1,
              initial_indent=''):
    """
    Deals with indentation of dictionaries with very long key, value pairs.
    replace_whitespace: Replace each whitespace character with a single space.
    break_long_words: If True words longer than width will be broken.
    width: The maximum length of wrapped lines.
    initial_indent: String that will be prepended to the first line of the output

    Wraps all strings for both keys and values to 120 chars.
    Uses 4 spaces indentation for both keys and values.
    Nested dictionaries and lists go to next line.
    """
    twr = TextWrapper(replace_whitespace=replace_whitespace,
                      break_long_words=break_long_words,
                      width=maxWidth,
                      initial_indent=initial_indent)

    def twEnclosed(obj, ind='', depthReached=0, reCall=False):
        """
        The inner function of the closure
        ind: Initial indentation for the single output string
        reCall: Flag to indicate a recursive call (should not be used outside)
        """
        output = ''
        if isinstance(obj, dict):
            obj = OrderedDict(sorted(obj.items(),
                                     key=lambda t: t[0],
                                     reverse=False))
            if reCall:
                output += '\n'
            ind += '    '
            depthReached += 1
            lengthReached = 0
            for key, value in obj.items():
                lengthReached += 1
                if lengthReached > maxLength and maxLength >= 0:
                    output += "%s...\n" % ind
                    break
                if depthReached <= maxDepth or maxDepth < 0:
                    output += "%s%s: %s" % (ind,
                                            ''.join(twr.wrap(key)),
                                            twEnclosed(value, ind, depthReached=depthReached, reCall=True))

        elif isinstance(obj, (list, set)):
            if reCall:
                output += '\n'
            ind += '    '
            lengthReached = 0
            for value in obj:
                lengthReached += 1
                if lengthReached > maxLength and maxLength >= 0:
                    output += "%s...\n" % ind
                    break
                if depthReached <= maxDepth or maxDepth < 0:
                    output += "%s%s" % (ind, twEnclosed(value, ind, depthReached=depthReached, reCall=True))
        else:
            output += "%s\n" % str(obj)  # join(twr.wrap(str(obj)))
        return output

    return twEnclosed


def twPrint(obj, maxWidth=120, maxLength=-1, maxDepth=-1):
    """
    A simple caller of twClosure (see docstring for twClosure)
    """
    twPrinter = twClosure(maxWidth=maxWidth,
                          maxLength=maxLength,
                          maxDepth=maxDepth)
    print(twPrinter(obj))


def twFormat(obj, maxWidth=120, maxLength=-1, maxDepth=-1):
    """
    A simple caller of twClosure (see docstring for twClosure)
    """
    twFormatter = twClosure(maxWidth=maxWidth,
                            maxLength=maxLength,
                            maxDepth=maxDepth)
    return twFormatter(obj)
