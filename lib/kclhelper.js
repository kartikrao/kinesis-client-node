#!env /usr/local/bin/node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var KCLHelper, argv, glob, kclhelper, mld_class, optimist, path, paths, usage, _;

  optimist = require('optimist');

  _ = require('underscore');

  path = require('path');

  glob = require('glob');

  KCLHelper = (function() {
    function KCLHelper() {
      this.kclpath = path.resolve("" + __dirname + "/../");
      this.separator = ":";
    }

    KCLHelper.prototype.getKclDir = function() {
      return this.kclpath;
    };

    KCLHelper.prototype.getKclJarPath = function(cb) {
      var self;
      self = this;
      glob("" + self.kclpath + "/jars/**/*.jar", function(err, files) {
        if (files == null) {
          files = [];
        }
        cb(files.join(self.separator));
      });
    };

    KCLHelper.prototype.getKclClasspath = function(propertyPath, paths, cb) {
      var p, rpaths, self, _i, _len;
      if (propertyPath == null) {
        propertyPath = null;
      }
      if (paths == null) {
        paths = [];
      }
      rpaths = [];
      for (_i = 0, _len = paths.length; _i < _len; _i++) {
        p = paths[_i];
        rpaths.push(path.resolve(p));
      }
      if ((propertyPath != null ? propertyPath.length : void 0) > 0) {
        rpaths.push(path.resolve(propertyPath));
      }
      self = this;
      this.getKclJarPath(function(jarpath) {
        var classpath;
        if (jarpath == null) {
          jarpath = [];
        }
        classpath = _.union(rpaths, jarpath);
        cb(classpath.join(self.separator));
      });
    };

    KCLHelper.prototype.getKclAppCommand = function(java, mld_class, properties, paths) {
      var baseName;
      if (paths == null) {
        paths = [];
      }
      baseName = properties;
      if (properties.indexOf("/") > -1) {
        baseName = properties.substring(properties.lastIndexOf("/") + 1);
      }
      return "" + java + " -cp " + (this.getKclClasspath(properties, paths)) + " " + mld_class + " " + baseName;
    };

    return KCLHelper;

  })();

  kclhelper = new KCLHelper;

  argv = optimist.boolean(["print_classpath", "print_command"]).argv;

  if (argv.sample != null) {
    if (argv.properties != null) {
      console.error("Replacing provided properties with sample properties due to arg --sample");
    }
    argv.properties = argv.sample.indexOf("/") > -1 ? argv.sample : "" + __dirname + "/" + argv.sample;
  }

  if (argv.print_classpath === true) {
    kclhelper.getKclClasspath(argv.properties, null, function(cp) {
      console.log(cp);
    });
  } else if (argv.print_command != null) {
    if ((argv.java != null) && (argv.properties != null)) {
      mld_class = "com.amazonaws.services.kinesis.multilang.MultiLangDaemon";
      paths = argv.paths != null ? argv.paths.split(",") : [];
      kclhelper.getKclAppCommand(argv.java, mld_class, argv.properties, paths);
    } else {
      console.error("Must provide arguments --java and --properties\n");
    }
  } else {
    usage = "--java  $path_to_java_executable - Required\n--props $rel_path_to_properties  - Required\n--print_classpath [false]        - Show classpath\n--print_command   [false]        - Show Java command\n--sample          [false]        - Use sample properties";
    console.error(usage);
  }

}).call(this);
