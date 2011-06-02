import json
from contextlib import contextmanager


class FileSink(object):
    """
    Class handles storing Alert messages (JSONized) into a file.
    
    File is being appended, there is no wrapping object representation,
    e.g. list, so direct loading of the entire file content into
    JSON is not possible, yet load() is implemented. 
    
    """
    
    
    def __init__(self, config):
        self.outputfile = config.outputfile
        self.encoder = json.encoder.JSONEncoder()
        self.decoder = json.decoder.JSONDecoder()


    @contextmanager
    def _handleFile(self, mode):
        f = open(self.outputfile, mode)
        try:
            yield f
        finally:
            f.close()
        
    
    def load(self):
        """
        Return a Python list of Alert instances loaded from the file this
        instace of FileSink is configured with.
        
        """
        r = []
        with self._handleFile('r') as f:
            for line in f:
                obj = self.decoder.decode(line)
                r.append(obj)
        return r
            
                    
    def send(self, alerts):
        """
        alerts is a list of Alert instances.
        
        Writes out new line separated json representation of Alert instances.
        The corresponding test tests that new line character is handled well
        if occurrs in the payload of the Alert instance.
        
        """
        with self._handleFile('a') as f:
            for a in alerts:
                s = self.encoder.encode(a)
                f.write("%s\n" % s)