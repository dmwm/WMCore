
from WMCore.Configuration import Configuration

config = Configuration()


# If used within a WMCore component, just include the RemoteMsg section
# in your WMCore configuration file (and don't forget to set the 
# config.RemoteMsg.inComponent attribute).


# RemoteMsg: 
config.section_('RemoteMsg')

# If in a WMCore component, set this to the appropriate name
#config.RemoteMsg.inComponent = 'MyComponent'

# Formatter class to use for message responses (or acknowledgments)
#config.RemoteMsg.formatter = 'RemoteMsg.DefaultFormatter'

# The configuration file read by cherrypy, it might even be empty
config.RemoteMsg.listenerConfig = 'sample_http.conf'

# Port where to listen for incoming messages
config.RemoteMsg.listenerPort = '8030'

# Uncomment if we should authenticate remote clients agains this user/pwd/realm
#config.RemoteMsg.listenerUser = 'a'
#config.RemoteMsg.listenerPwd = 'pwda'
#config.RemoteMsg.listenerRealm = 'RemoteMsg'

# Port in remote servers to which we should send messages as client
config.RemoteMsg.senderPort = '8010'

# Uncomment if we should this user/pwd/realm when sending msgs to remote servers
#config.RemoteMsg.senderUser = 'b'
#config.RemoteMsg.senderPwd = 'pwdb'
#config.RemoteMsg.senderRealm = 'RemoteMsg'

# Log level
config.RemoteMsg.logLevel = 'CRITICAL'
#config.RemoteMsg.logLevel = 'DEBUG'

# Dir where by default all logs go (if following individual files are not set)
config.RemoteMsg.RemoteMsgDir = '/tmp/RemoteMsg'

# Log file for the Remote Msg objects
config.RemoteMsg.LogFile = '/tmp/RemoteMsg/remoteMsg.log'

# Log file for the cherrypy server
config.RemoteMsg.listenerLogFile = '/tmp/RemoteMsg/listener.log'
