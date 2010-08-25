class ServiceInterface:
    
    def __init__(self, restModel):
        self.model = restModel
        
    def register(self):
        """
        """
        raise NotImplementedError, "register method is not implemented"