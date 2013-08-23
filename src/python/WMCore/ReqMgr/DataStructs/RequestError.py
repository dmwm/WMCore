from WMCore.REST.Error import RESTError

class InvalidStateTransition(RESTError):
    "The specified object is invalid."
    http_code = 400
    app_code = 1101
    
    def __init__(self, current_state, new_state):
        RESTError.__init__(self)
        self.message = "InvalidStatus Transition: %s to %s" % (current_state, new_state)