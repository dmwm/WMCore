#!/usr/bin/env python
"""
Module containing mathematical and physics utils
"""


from builtins import int, str
from math import ceil


def quantize(inputVal, quanta):
    """
    _quantize_

    Quantize the input value following the quanta provided.
    """
    if isinstance(inputVal, str):
        inputVal = float(inputVal)
    elif not isinstance(inputVal, (int, float)):
        msg = "Input value has to be either int or float, not %s" % (type(inputVal))
        raise ValueError(msg)

    if isinstance(quanta, (str, int, float)):
        quanta = int(float(quanta))
    elif not isinstance(quanta, int):
        msg = "Quanta value has to be either int or float, not %s" % (type(quanta))
        raise ValueError(msg)

    res = int(ceil(inputVal / quanta))

    return res * quanta
