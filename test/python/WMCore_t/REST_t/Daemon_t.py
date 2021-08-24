# python-future
from __future__ import print_function
from builtins import range

# system modules
import cherrypy
from multiprocessing import Process
from cheroot.test import webtest
from cherrypy import process
from threading import Thread, Condition
import time, random

# WMCore modules
from WMCore.REST.Test import setup_dummy_server, fake_authz_headers
from WMCore.REST.Test import fake_authz_key_file
from WMCore.REST.Server import RESTApi, RESTEntity, restcall

FAKE_FILE = fake_authz_key_file()
PORT = 8889

class Task(Thread):
    """A pseudo-task which runs in a separate thread. Provides standard
    functionality to start and stop with CherryPy engine transitions,
    and to get a "daemon state" object. In this case the status is just
    a series of time stamps when the daemon pretended to last do work."""

    def __init__(self):
        Thread.__init__(self, name = "Task")
        self._cv = Condition()
        self._status = {}
        self._stopme = False
        cherrypy.engine.subscribe("stop", self.stop)
        if cherrypy.engine.state == process.wspbus.states.STOPPED:
            cherrypy.engine.subscribe("start", self.start)
        else:
            self.start()

    def status(self):
        """Get the daemon status. Returns dictionary of time stamps of the
        the last hundred times this thread last did 'work'."""
        with self._cv:
            return dict((k, v) for k, v in self._status.items())

    def stop(self):
        """Tell the task thread to quit."""
        with self._cv:
            self._stopme = True
            self._cv.notifyAll()

    def run(self):
        """Run the task thread work routine. This just sleeps random [0,.5]
        seconds, and marks the time into the status dictionary, of course
        all in a thread-safe manner."""
        n = 0
        with self._cv:
            while not self._stopme:
                n += 1
                if n % 100 == 0:
                    self._status = {}
                    n = 0
                self._status[str(n)] = time.time()
                self._cv.wait(random.random() * 0.5)

class Status(RESTEntity):
    """REST entity to retrieve the status of running tasks."""
    def __init__(self, app, api, config, mount, tasks):
        """:arg list tasks: the list of task objects."""
        RESTEntity.__init__(self, app, api, config, mount)
        self._tasks = tasks

    def validate(self, *args):
        """No arguments, no validation required."""
        print("AMR validate executed")
        pass

    @staticmethod
    def gather(tasks):
        """Helper function which returns the status of each task,
        producing results as a generator."""
        for task in tasks:
            yield task.status()

    @restcall
    def get(self):
        """Get the status of all our registered tasks."""
        print("AMR status get request received")
        return self.gather(self._tasks)

class TaskAPI(RESTApi):
    """REST API which runs a bunch of pseudo tasks, and provides a REST
    entity to report their status via HTTP GET."""
    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)
        print("AMR mounting RESTApi app: %s, config: %s, mount: %s" % (app, config, mount))
        tasks = [Task() for _ in range(0, 10)]
        self._add({ "status": Status(app, self, config, mount, tasks) })
        print("AMR done mounting the 'status' API")


class TaskTest(webtest.WebCase):
    """Client to verify :class:`TaskAPI` works."""
    def setUp(self):
        self.h = fake_authz_headers(FAKE_FILE.data)
        webtest.WebCase.PORT = PORT
        self.engine = cherrypy.engine
        self.proc = load_server(self.engine)
        print("AMR server loaded")

    def tearDown(self):
        stop_server(self.proc, self.engine)

    def test(self):
        h = self.h
        h.append(("Accept", "application/json"))
        print("AMR headers: %s" % h)
        print(self.getPage("/test", headers=h))
        print(self.getPage("/test/status", headers=h))
        for _ in range(0, 10):
            self.getPage("/test/status", headers=h)
            print(self.bodyY)
            time.sleep(.3)

def setup_server():
    srcfile = __file__.split("/")[-1].split(".py")[0]
    print("AMR srcfile : %s" % srcfile)
    setup_dummy_server(srcfile, "TaskAPI", authz_key_file=FAKE_FILE, port=PORT)

def load_server(engine):
    setup_server()
    proc = Process(target=start_server, name="cherrypy_Api_t", args=(engine,))
    proc.start()
    proc.join(timeout=1)
    return proc

def start_server(engine):
    webtest.WebCase.PORT = PORT
    cherrypy.log.screen = True
    engine.start()
    engine.block()

def stop_server(proc, engine):
    cherrypy.log.screen = True
    engine.stop()
    proc.terminate()

if __name__ == '__main__':
    webtest.main()
