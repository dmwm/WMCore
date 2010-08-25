from openid.extensions.sreg import *

# The data fields that are listed in the sreg spec
data_fields.update({'permissions': 'Permissions',
                    'dn': 'Distinguished Name'})


__all__ = [
    'SRegRequest',
    'SRegResponse',
    'sendSRegFields',
    'data_fields',
    'ns_uri',
    'supportsSReg',
    ]
