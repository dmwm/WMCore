from WMCore.REST.Format import RESTFormat
from WMCore.REST.Format import XMLFormat
from WMCore.REST.Format import JSONFormat
from WMCore.REST.Format import RawFormat
from WMCore.REST.Format import DigestETag
from WMCore.REST.Format import MD5ETag
from WMCore.REST.Format import SHA1ETag
RESTFormat()
XMLFormat("app")
JSONFormat()
RawFormat()
DigestETag('md5')
MD5ETag()
SHA1ETag()
