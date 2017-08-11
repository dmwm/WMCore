"""
File       : Regexp.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Module for common regular expressions
"""
# futures
from __future__ import print_function, division

# system modules
import re

# global regexp
PAT_QUERY = re.compile(r"^[a-zA-Z]+")
PAT_INFO = re.compile(r"^[0-9]+$")
PAT_UID = re.compile(r"^[a-z0-9]{32,32}$")

# time patterns
PAT_YYYYMMDD = re.compile(r'^20[0-9][0-9][0-1][0-9][0-3][0-9]$')
PAT_YYYY = re.compile(r'^20[0-9][0-9]$')
PAT_MM = re.compile(r'^(0[1-9]|1[012])$')
PAT_DD = re.compile(r'^(0[1-9]|[12][0-9]|3[01])$')

# number patterns
PAT_FLOAT = re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')
PAT_INT = re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
