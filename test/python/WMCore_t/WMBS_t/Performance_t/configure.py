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
                      help="Conect to the database as USER")
    parser.add_option("-h", "--host", dest="host", metavar="HOST", 
                      help="Database is running on HOST")
    
    return parser.parse_args()

def config(inifile, options):
    config = ConfigParser ()
    config.add_section("database")
    
    config.set("database", "user", options.user)
    config.set("database", "host", options.host)
    config.set("database", "instance", options.inst)
    
    config.write(file ('%.ini' % options.dbtype, 'w'))

def main():
    options, args = input()
    config(options)
    setReadOnly(options.inifile)
    if options.schema:
        database(options.inifile)
        
if __name__ == "__main__":
    main()   