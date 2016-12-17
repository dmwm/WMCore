#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : XMLUtils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of utilities for RequestManager code
"""

from __future__ import (division, print_function)
# system modules
import re
import cStringIO as StringIO
import xml.etree.cElementTree as ET

int_number_pattern = re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
float_number_pattern = re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')

def adjust_value(value):
    """
    Change null value to None.
    """
    pat_float   = float_number_pattern
    pat_integer = int_number_pattern
    if  isinstance(value, str):
        if  value == 'null' or value == '(null)':
            return None
        elif pat_float.match(value):
            return float(value)
        elif pat_integer.match(value):
            return int(value)
        else:
            return value
    else:
        return value
    
def xml_parser(data, prim_key, tags=None):
    "Generic XML parser"
    if  isinstance(data, basestring):
        stream = StringIO.StringIO()
        stream.write(data)
        stream.seek(0)
    else:
        stream = data
    context = ET.iterparse(stream, events=("start", "end"))
    root = None
    sup = {}
    for item in context:
        event, elem = item
        if  event == "start" and root is None:
            root = elem # the first element is root
        row = {}
        if  tags and not sup:
            for tag in tags:
                if  tag.find(".") != -1:
                    atag, attr = tag.split(".")
                    if  elem.tag == atag and attr in elem.attrib:
                        att_value = elem.attrib[attr]
                        if  isinstance(att_value, dict):
                            att_value = elem.attrib[attr]
                        if  isinstance(att_value, str):
                            att_value = adjust_value(att_value)
                        sup[atag] = {attr:att_value}
                else:
                    if  elem.tag == tag:
                        sup[tag] = elem.attrib
        key = elem.tag
        if  key != prim_key:
            continue
        row[key] = elem.attrib
        row[key].update(sup)
        get_children(elem, event, row, key)
        if  event == 'end':
            elem.clear()
            yield row
    if  root:
        root.clear()

def get_children(elem, event, row, key):
    """
    xml_parser helper function. It gets recursively information about
    children for given element tag. Information is stored into provided
    row for given key. The change of notations can be applied during
    parsing step by using provided notations dictionary.
    """
    for child in elem.getchildren():
        child_key  = child.tag
        child_data = child.attrib
        if  not child_data:
            child_dict = adjust_value(child.text)
        else:
            child_dict = child_data

        if  isinstance(row[key], dict) and child_key in row[key]:
            val = row[key][child_key]
            if  isinstance(val, list):
                val.append(child_dict)
                row[key][child_key] = val
            else:
                row[key][child_key] = [val] + [child_dict]
        else:
            if  child.getchildren(): # we got grand-children
                if  child_dict:
                    row[key][child_key] = child_dict
                else:
                    row[key][child_key] = {}
                if  isinstance(child_dict, dict):
                    newdict = {child_key: child_dict}
                else:
                    newdict = {child_key: {}}
                get_children(child, event, newdict, child_key)
                row[key][child_key] = newdict[child_key]
            else:
                if  not isinstance(row[key], dict):
                    row[key] = {}
                row[key][child_key] = child_dict
        if  event == 'end':
            child.clear()
