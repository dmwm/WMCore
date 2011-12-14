import logging
    
class SequentialTaskBase(object):
    
    def __init__(self, *args, **kwargs):
        self.initialize(*args, **kwargs)
        self.setCallSequence()
        
    def __call__(self, stopFlagFunc):
        for call in self._callSequence:
            if stopFlagFunc():
                return
            try:
                call()
            except Exception, ex:
                #log the excpeiotn and break. 
                #SequencialTasks are interconnected between functions  
                logging.error(str(ex))
                print (str(ex))
                break
            
    def initialize(self):
        raise NotImplementedError("Initialize args for the member functions")
    
    def setCallSequence(self):
        """
        set the list of function call with out args on self.callSequence
        
        i.e.
        self.callSequence = [self.do_something1, self.do_something1]
        """
        raise NotImplementedError("need to implement setCallSequence assign self._callSequence")
