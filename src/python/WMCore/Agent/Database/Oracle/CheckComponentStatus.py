"""
_CheckComponentStatus_

Oracle implementation of CheckComponentStatus
"""

__all__ = []



from WMCore.Agent.Database.MySQL.CheckComponentStatus import CheckComponentStatus \
     as CheckComponentStatusMySQL

class CheckComponentStatus(CheckComponentStatusMySQL):
    pass
