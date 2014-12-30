from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import \
        SequentialTaskBase, PeriodicWorker, CherryPyPeriodicTask


class Hello(SequentialTaskBase):
    
    def setCallSequence(self):
        self._callSequence = [self.printHello, self.printThere, self.printA]
    
    def printHello(self, config):
        print "Hello"
        
    def printThere(self, config):
        print "there"
    
    def printA(self, config):
        print "A"
    

def sayHello(config):
    print "Hi Hello"

def sayBye(config):
    print "Bye"

class WMDataMining(CherryPyPeriodicTask):
    
    def __init__(self, rest, config):
        
        CherryPyPeriodicTask.__init__(self, config)
        
    def setConcurrentTasks(self, config):
        """
        sets the list of functions which 
        """
        self.concurrentTasks = [{'func': sayBye, 'duration': config.activeDuration}, 
                                {'func': Hello(), 'duration': config.archiveDuration}] 
        

if __name__ == '__main__':
    import cherrypy
    from WMCore.Configuration import Configuration
    config = Configuration()
    config.section_("wmmining")
    config.wmmining.activeDuration = 5
    config.wmmining.archiveDuration = 30
    
    #helloTask = PeriodicWorker(sayHello, config.wmmining)
    WMDataMining(None, config.wmmining)
    cherrypy.quickstart()