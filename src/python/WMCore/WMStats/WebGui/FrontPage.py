"""
Main ReqMgr web page handler.

"""
import re
from WMCore.REST.Server import RESTFrontPage

# path to static resources
RX_STATIC_DIR_PATH = re.compile(r"^([a-zA-Z]+/)+[-a-z0-9_]+\.(?:css|js|png|gif|html)$")

class FrontPage(RESTFrontPage):

    def __init__(self, app, config, mount):
        """
        :arg app: reference to the application object.
        :arg config: reference to the configuration.
        :arg str mount: URL mount point.

        """

        # must be in a static content directory
        frontpage = "html/WMStats/index.html"
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

                "rx": RX_STATIC_DIR_PATH
            },
            "js":
            {   "root": "%s/html/WMStats/js/" % config.static_content_dir,
                "rx": re.compile(r"^([a-zA-Z]+/)+[-a-z0-9_]+\.(?:js)$")
            },
            "css":
            {   "root": "%s/html/WMStats/css/" % config.static_content_dir,
                "rx": re.compile(r"^([a-zA-Z]+/)+[-a-z0-9_]+\.(?:css)$")
            },
            "images":
            {   "root": "%s/html/WMStats/images/" % config.static_content_dir,
                "rx": re.compile(r"^([a-zA-Z]+/)+[-a-z0-9_]+\.(?:png|gif)$")
            },
            "lib":
            {   "root": "%s/html/WMStats/lib/" % config.static_content_dir,
                "rx": RX_STATIC_DIR_PATH
            },
            "fonts":
            {   "root": "%s/html/WMStats/fonts/" % config.static_content_dir,
                "rx": RX_STATIC_DIR_PATH
            }

        }
        RESTFrontPage.__init__(self, app, config, mount, frontpage, roots)
