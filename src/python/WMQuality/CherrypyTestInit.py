from __future__ import print_function

import threading
import traceback

import cherrypy


def start(server):
    try:
        server.start(blocking=False)
    except RuntimeError as e:
        # there appears to be worker threads from a previous test
        # hanging out. Try to slay them so that we can keep going
        print("Failed to load cherrypy with exception: %s\n" % e)
        print("The threads are: \n%s\n" % threading.enumerate())
        print("The previous test was %s\n" % server.getLastTest())
        print(traceback.format_exc())
        server.stop()
        raise e


def stop(server):
    server.stop()
    server.setLastTest()
    cherrypy.engine.exit()
    return