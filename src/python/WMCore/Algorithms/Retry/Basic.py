'''
Base class for retry algorithms. Does nothing but repeat the given command N 
times.

Written at Bink Boink studios while recording the first Uphills EP...
'''
from WMCore.WMException import WMException

class RetryFailException(WMException):
    def __init__(self, message):
        WMException.__init__(self, message, 'WMCORE-13')

class Basic:
    def __init__(self, timeout = 1, max = 10, unit = 1):
        """
        timeout is how long to wait, max is the number of retries, unit is 
        multiple of seconds
        """
        self.timeout = timeout
        self.max = max
        self.unit = unit
        self.count = 0
        self.name = 'Base'
        
    def pre(self):
        """
        Do something before running a command
        """
        pass
    
    def post(self):
        """
        Do something after running a command
        """
        pass
    
    def run(self, function, *args, **kwargs):
        """
        Run function between pre and post. Function must throw an exception if 
        it fails, otherwise it's response will be returned. If the retry count 
        exceeds self.max the function will raise an exception. It is expected 
        that any logging is managed by the called function, so why the function 
        failed is up to that function to record.  
        """
        while True:
            self.pre()
            try:
                retval = function(args, kwargs)
                return retval
            except:
                self.post()
                self.count += 1
            if self.count >= self.max:
                # Sometimes you just have to know when to give up...
                raise RetryFailException('%s: Number of retries (%s) exceeded for %s' % 
                                         (self.name, self.max, str(function)))