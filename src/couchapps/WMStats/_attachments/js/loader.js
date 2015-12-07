// Work around different mount points of couchdb
// so that it can run behind the frontend, with
// or without rewrites.
// dbpath, dbname, couchroot global variable which used in jquery.couch.js library
// So shouldn't change the name.
dbpath = document.location.href.split('/_design')[0];
dbpath = dbpath.split('/index.html')[0];
wmstatsUrlComponents = dbpath.split('/');
dbname = wmstatsUrlComponents[wmstatsUrlComponents.length - 1];
couchroot = dbpath.substring(0,dbpath.lastIndexOf('/'));
//hack for support reqmgr_workload_config db access : change reqmgr rewrite rule
if (couchroot.match(/:\d+$/) == null && couchroot.indexOf("couchdb") == -1) {couchroot += "/couchdb";};
function couchapp_load(scripts) {
  for (var i=0; i < scripts.length; i++) {
    document.write('<script src="'+scripts[i]+'"><\/script>');
  };
};

couchapp_load([
   "vendor/couchapp/loader.js",
   "vendor/jquery/jquery-ui.min.js",
   "vendor/datatables/jquery.dataTables.min.js",
   "vendor/datatables/ColVis.min.js",
   "lib/namespace.js",
   "lib/jquery.dataTables.columnFilter.js"
]);
