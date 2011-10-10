#! /usr/bin/env python

"""
_MiscAlgos_

Useful little tools that can be handy anywhere.
"""
import logging

def sortListByKey(input, key):
    """
    Return list of dictionaries as a
    dictionary of lists of dictionaries
    keyed by one original key

    """
    final = {}

    for entry in input:
        value = entry.get(key, None)
        if value == None:
            # Empty dict value?
            # This is an error, but we can't handle it here
            logging.error("Found entry with no key in sortListByKey: %s" % entry)
            logging.error("Skipping")
            continue
        if type(value) == set:
            try:
                v     = value
                value = v.pop()
                v.add(value)
            except KeyError:
                # Set was empty?
                # This is peculiar, we can't handle this.
                logging.error("Found list entry with empty key set in sortListByKey: %s" % entry)
                logging.error("Skipping")
                continue
        if not value in final.keys():
            final[value] = []
        final[value].append(entry)

    return final



