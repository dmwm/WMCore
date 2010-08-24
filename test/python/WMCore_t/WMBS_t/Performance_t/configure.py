#!/usr/bin/env python
"""
A simple script to write an ini file for tests to use. Currently MySQL 
orientated
"""

from ConfigParser import ConfigParser
from optparse import OptionParser

def input():
    parser = OptionParser()
    parser.add_option("-d", "--dialect", dest="dbtype", default='mysql',
                      help="Create ini for database of TYPE", metavar="TYPE")
    parser.add_option("-i", "--instance", dest="inst", default='wmbs',
                      help="User INSTANCE", metavar="INSTANCE")
    parser.add_option("-u", "--user", dest="user", metavar="USER",
                      help="Connect to the database as USER")
    parser.add_option("-o", "--host", dest="host", metavar="HOST", 
                      help="Database is running on HOST")
    parser.add_option("-v", "--verbose", dest="verbose", default=False,
                      help="Set query verbose mode", action="store_true")
 
    return parser.parse_args()

def config(options):
    config = ConfigParser ()
    config.add_section("output")    
    config.add_section("database")

    config.set("output", "verbose", options.verbose)
    config.set("database", "user", options.user)
    config.set("database", "host", options.host)
    config.set("database", "instance", options.inst)
    
    config.write(file ('%s.ini' % options.dbtype, 'w'))

def main():
    options, args = input()
    config(options)
    
if __name__ == "__main__":
    main()   
