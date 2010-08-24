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
    parser.add_option("-r", "--threshold", dest="threshold", metavar="THRESHOLD", 
                      help="Threshold time for a single DAO operation")
    parser.add_option("-e", "--totalthreshold", dest="total_threshold", metavar="TOTALTHRESHOLD", 
                      help="Threshold time for cumulative DAO operations")
    parser.add_option("-t", "--times", dest="times", metavar="TIMES", 
                      help="Number of times each test should be executed")
    parser.add_option("-v", "--verbose", dest="verbose", default=False,
                      help="Set query verbose mode", action="store_true")
 
    return parser.parse_args()

def config(options):
    config = ConfigParser ()

    config.add_section("output")
    config.add_section("settings")
    
    config.add_section("mysql")
    config.add_section("sqlite")
    config.add_section("oracle")

    #Verbose Mode ON/OFF
    config.set("output", "verbose", options.verbose)

    #Testcases specific settings
    config.set("settings", "threshold", options.threshold)
    config.set("settings", "total_threshold", options.total_threshold)
    config.set("settings", "times", options.times)

    #MySQL
    config.set("mysql", "user", options.user)
    config.set("mysql", "host", options.host)
    config.set("mysql", "instance", options.inst)

    #SQLite
    config.set("sqlite", "user", options.user)
    config.set("sqlite", "host", options.host)
    config.set("sqlite", "instance", options.inst)

    #Oracle

    config.set("oracle", "user", options.user)
    config.set("oracle", "host", options.host)
    config.set("oracle", "instance", options.inst)
    
    config.write(file ('test.ini', 'w'))

def main():
    options, args = input()
    config(options)
    
if __name__ == "__main__":
    main()   
