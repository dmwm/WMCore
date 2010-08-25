#!/usr/local/bin/python
"""
__Constants__

Contains constants for TaskQueue modules use.
These are not meant to be changed (if there is a 
need for it, those variables should be converted
to conf variables).
 
"""

# Do not change next lines (used by cherrypy to server/accept files)
staticRoot = 'static'
uploadRoot = 'upload'

# TODO: Should this be loaded from DB or uploaded from conf file?
#       => conf file probably not. DB... Is it any better than having it here?
taskStates = {'Queued': 0, 'Assigned': 1, 'Running': 2, 'Done': 3, 'Failed': 4}


# These could basically be anything
# (both server and client read it and use it)
# No apparent need to make them configurable for now
sandboxUrlDir = "sandbox" 
specUrlDir = "spec" 
reportUrlDir = "reports"
pilotLogUrlDir = "logs"


# TODO: The rank expression might be made configurable (or not??)
RANK_EXPR = "1000*('id' in reqs) + 500*('host' in reqs) + 100*('se' in reqs)"
RANK_EXPR += "+ reduce(lambda x,y: x+y, map(lambda x: x in reqs, cache), 0)"
#RANK_EXPR = "1000"
