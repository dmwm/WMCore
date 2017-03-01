"""
Main ReqMgr web page handler.

"""

from WMCore.REST.Server import RESTFrontPage

import WMCore.ReqMgr.Service.RegExp as rx


class FrontPage(RESTFrontPage):

    def __init__(self, app, config, mount):
        """
        :arg app: reference to the application object.
        :arg config: reference to the configuration.
        :arg str mount: URL mount point.

        """

        # must be in a static content directory
        frontpage = "html/ReqMgr/index.html"
        roots = \
        {
            "html":
            {
                # without repeating the 'html' here, it doesn't work
                # due to path gymnastics in WMCore.REST.Server.py
                # rather messy figuring out static content dir by
                # counting file separators and making it compatible
                # between localhost and VM running, hence the config
                # value here
                "root": "%s/html/" % config.static_content_dir,

                "rx": rx.RX_STATIC_DIR_PATH
            },
        }
        RESTFrontPage.__init__(self, app, config, mount, frontpage, roots)
