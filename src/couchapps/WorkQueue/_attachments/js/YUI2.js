//Create the global YUI object to load YUI
WQ.YUI = function () {

    return YUI({
         base: "point/to/yui3/lib",
             combine: false,
             groups: {
                yui2: {
                    //change to correct path to point local lib
                    base:      'local/path/to/2in3/dist/2.8.1/build/',
                    patterns:  {
                        'yui2-': {
                            configFn: function(me) {
                                if(/-skin|reset|fonts|grids|base/.test(me.name)) {
                                    me.type = 'css';
                                    me.path = me.path.replace(/\.js/, '.css');
                                    // this makes skins in builds earlier than 2.6.0 work as long as combine is false
                                    me.path = me.path.replace(/\/yui2-skin/, '/assets/skins/sam/yui2-skin');
                                }
                            }
                        }
                    }
                },
                wmcore: {
                    //change to correct path to point local lib
                    base:      'javascript/',
                    patterns:  {
                        'wmcore-': {
                            configFn: function(me) {
                                if(/-skin|reset|fonts|grids|base/.test(me.name)) {
                                    me.type = 'css';
                                    me.path = me.path.replace(/\.js/, '.css');
                                    // this makes skins in builds earlier than 2.6.0 work as long as combine is false
                                    me.path = me.path.replace(/\/yui2-skin/, '/assets/skins/sam/yui2-skin');
                                }
                            }
                        }
                    }
                }
             }
          });
    }