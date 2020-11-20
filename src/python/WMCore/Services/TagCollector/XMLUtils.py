#!/usr/bin/env python
# encoding: utf-8

"""
File       : XMLUtils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of utilities for RequestManager code
"""

from __future__ import (division, print_function)

from io import BytesIO
import re
import xml.etree.cElementTree as ET

int_number_pattern = re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
float_number_pattern = re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')


def adjust_value(value):
    """
    Change null value to None.
    """
    pat_float = float_number_pattern
    pat_integer = int_number_pattern
    if isinstance(value, str):
        if value == 'null' or value == '(null)':
            return None
        elif pat_float.match(value):
            return float(value)
        elif pat_integer.match(value):
            return int(value)
        else:
            return value
    else:
        return value


def xml_parser(data, prim_key):
    "Generic XML parser"
    if isinstance(data, basestring):
        stream = BytesIO()
        stream.write(data)
        stream.seek(0)
    else:
        stream = data

    context = ET.iterparse(stream)
    for event, elem in context:
        row = {}
        key = elem.tag
        if key != prim_key:
            continue
        row[key] = elem.attrib
        get_children(elem, event, row, key)
        elem.clear()
        yield row


def get_children(elem, event, row, key):
    """
    xml_parser helper function. It gets recursively information about
    children for given element tag. Information is stored into provided
    row for given key. The change of notations can be applied during
    parsing step by using provided notations dictionary.
    """
    for child in elem.getchildren():
        child_key = child.tag
        child_data = child.attrib
        if not child_data:
            child_dict = adjust_value(child.text)
        else:
            child_dict = child_data

        if child.getchildren():  # we got grand-children
            if child_dict:
                row[key][child_key] = child_dict
            else:
                row[key][child_key] = {}
            if isinstance(child_dict, dict):
                newdict = {child_key: child_dict}
            else:
                newdict = {child_key: {}}
            get_children(child, event, newdict, child_key)
            row[key][child_key] = newdict[child_key]
        else:
            if not isinstance(row[key], dict):
                row[key] = {}
            row[key].setdefault(child_key, [])
            row[key][child_key].append(child_dict)
        child.clear()
