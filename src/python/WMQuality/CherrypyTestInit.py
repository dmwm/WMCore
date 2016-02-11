from __future__ import print_function
import threading
import traceback
import cherrypy
import cherrypy.process.wspbus as cherrybus

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
    # there was a ton of racy failures in REST tools because of
    # how much global state cherrypy has. this resets it

    # Also, it sucks I had to copy/paste this from
    # https://bitbucket.org/cherrypy/cherrypy/src/9720342ad159/cherrypy/__init__.py
    # but reload() doesn't have the right semantics

    cherrybus.bus = cherrybus.Bus()
    cherrypy.engine = cherrybus.bus
    cherrypy.engine.timeout_monitor = cherrypy._TimeoutMonitor(cherrypy.engine)
    cherrypy.engine.timeout_monitor.subscribe()

    cherrypy.engine.autoreload = cherrypy.process.plugins.Autoreloader(cherrypy.engine)
    cherrypy.engine.autoreload.subscribe()

    cherrypy.engine.thread_manager = cherrypy.process.plugins.ThreadManager(cherrypy.engine)
    cherrypy.engine.thread_manager.subscribe()

    cherrypy.engine.signal_handler = cherrypy.process.plugins.SignalHandler(cherrypy.engine)
    cherrypy.engine.subscribe('log', cherrypy._buslog)

    from cherrypy import _cpserver
    cherrypy.server = _cpserver.Server()
    cherrypy.server.subscribe()
    cherrypy.checker = cherrypy._cpchecker.Checker()
    cherrypy.engine.subscribe('start', cherrypy.checker)