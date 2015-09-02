from cherrypy.test import helper
from WMCore.REST.Test import setup_test_server, fake_authz_headers
from WMCore.REST.Server import RESTApi, RESTEntity, restcall
from cherrypy import expose, engine, process, config as cpconfig
from threading import Thread, Condition
import WMCore.REST.Test as T
import time, random

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
        engine.subscribe("stop", self.stop)
        if engine.state == process.wspbus.states.STOPPED:
            engine.subscribe("start", self.start)
        else:
            self.start()

    def status(self):
        """Get the daemon status. Returns dictionary of time stamps of the
        the last hundred times this thread last did 'work'."""
        with self._cv:
            return dict((k, v) for k, v in self._status.iteritems())

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
        return self.gather(self._tasks)

class TaskAPI(RESTApi):
    """REST API which runs a bunch of pseudo tasks, and provides a REST
    entity to report their status via HTTP GET."""
    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)
        tasks = [Task() for _ in xrange(0, 10)]
        self._add({ "status": Status(app, self, config, mount, tasks) })

class TaskTest(helper.CPWebCase):
    """Client to verify :class:`TaskAPI` works."""
    def test(self):
        h = fake_authz_headers(T.test_authz_key.data)
        h.append(("Accept", "application/json"))
        for _ in xrange(0, 10):
            self.getPage("/test/status", headers=h)
            print self.body
            time.sleep(.3)

def setup_server():
    """Set up this test case."""
    srcfile = __file__.split("/")[-1].split(".py")[0]
    setup_test_server(srcfile, "TaskAPI")
    #cpconfig.update({"log.screen": True})
    #print server.config

if __name__ == '__main__':
    setup_server()
    helper.testmain()
