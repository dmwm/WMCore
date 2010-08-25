#!/bin/env python

"""
_BasicAlgos_

Python implementations of basic Linux functionality

"""

import os








def tail(filename, nLines = 20):
    """
    _tail_

    A version of tail
    Adapted from code on http://stackoverflow.com/questions/136168/get-last-n-lines-of-a-file-with-python-similar-to-tail
    """


    f = open(filename, 'r')

    assert nLines >= 0
    pos, lines = nLines+1, []
    while len(lines) <= nLines:
        try:
                f.seek(-pos, 2)
        except IOError:
                f.seek(0)
                break
        finally:
                lines = list(f)
        pos *= 2


    f.close()
        
    return lines[-nLines:]


