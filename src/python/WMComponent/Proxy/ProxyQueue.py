
"""
Wrapper for the proxy queue. This will eventually
interface with a persistent backend and make outbound
calls to ther prodagent instance. 2 instances per proxy
are instantiated to represent the in and outbound channel
between them.
"""

#FIXME: for now the proxy queue is just a direct interface
# to another database using the message service.
# This is a prodagent project dependency.
from WMComponent.Proxy.ProxyMsgs import ProxyMsgs


class ProxyQueue:
    """
    Wrapper for the proxy queue. This will eventually
    interface with a persistent backend and make outbound
    calls to other prodagent instance. 2 instances per proxy
    are instantiated to represent the in and outbound channel
    between them.
    """

    def __init__(self, name,details):
        self.details = details
        self.name = name
        self.proxyMsgs = ProxyMsgs(details['contact'])
        self.proxyMsgs.registerAs(name)

    def insert(self, msg):
        # we distinguish between certain messages if necessary
        if msg['name'] == 'ProxySubscribe':
            self.proxyMsgs.subscribeTo(msg['payload'])
        else:
            if not msg.has_key('delay'):
                msg['delay'] = "00:00:00"
            self.proxyMsgs.publish(msg['name'],msg['payload'],msg['delay'])

    def retrieve(self):
        # we can put wait on true or false .
        # BUT: be careful. wait = True means the thread
        # does not exit.
        type, payload = self.proxyMsgs.get(wait = False)
        return (type,payload)
