from openid.extensions.sreg import *

# The data fields that are listed in the sreg spec
data_fields.update({'site': 'Site', 
    'role': 'Role', 
    'group': 'Group'})
data_fields.update({'permissions': 'Roles', 
    'dn': 'DN'})

__all__ = [
    'SRegRequest',
    'SRegResponse',
    'sendSRegFields',
    'data_fields',
    'ns_uri',
    'supportsSReg',
    ]
