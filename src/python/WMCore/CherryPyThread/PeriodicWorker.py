import cherrypy
from threading import Thread, Condition

class PeriodicWorker(Thread):
    
    def __init__(self, func, duration=600):
        # use default RLock from condition
        # Lock wan't be shared between the instance used  only for wait
        # func : function or callable object pointer
        self.wakeUp = Condition()
        self.stopFlag = False
        self.taskFunc = func
        self.duration = duration
        try: 
            name = func.__class__.__name__
            print name
        except:
            name = func.__name__
            print name
        Thread.__init__(self, name=name)
        cherrypy.engine.subscribe('start', self.start, priority = 100)
        cherrypy.engine.subscribe('stop', self.stop, priority = 100)
        # not sure this is salf or needed
        # in case the there are long lasting work 
        # it allows python process to exit.
        #self.daemon = True

    def stop(self):
        self.wakeUp.acquire()
        self.stopFlag = True
        self.wakeUp.notifyAll()
        self.wakeUp.release()
    
    def isStopFlagOn(self):
        # this function can be used if the work needs to be gracefully 
        # shut down by setting the several stopping point in the self.taskFunc
        return self.stopFlag
    
    def run(self):
        while not self.stopFlag:
            self.wakeUp.acquire()
            self.taskFunc(self.isStopFlagOn)
            self.wakeUp.wait(self.duration)
            self.wakeUp.release()
        