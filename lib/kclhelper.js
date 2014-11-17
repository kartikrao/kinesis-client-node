#!env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var KCLHelper, args, argv, kclhelper, mld_class, optimist, path, paths, usage, _;

  path = require('path');

  optimist = require('optimist');

  _ = require('underscore');

  KCLHelper = (function() {
    function KCLHelper() {
      this.kclpath = __dirname;
      this.separator = ":";
    }

    KCLHelper.prototype.getKclDir = function() {
      return this.kclpath;
    };

    KCLHelper.prototype.getKclJarPath = function(cb) {
      var self;
      self = this;
      glob("" + this.kclpath + "/jars/**/*jar", function(err, files) {
        if (files == null) {
          files = [];
        }
        cb(files.join(self.separator));
      });
    };

    KCLHelper.prototype.getKclClasspath = function(propertyPath, paths, cb) {
      var rpaths, self, _i, _len;
      if (propertyPath == null) {
        propertyPath = null;
      }
      rpaths = [];
      for (_i = 0, _len = paths.length; _i < _len; _i++) {
        path = paths[_i];
        rpaths.push(path.resolve(path));
      }
      if (propertyPath != null) {
        rpaths.push(path.resolve(propertyPath));
      }
      self = this;
      this.getKclJarPath(function(jarpath) {
        var classpath;
        if (jarpath == null) {
          jarpath = [];
        }
        classpath = Array.concat(rpaths, jarpath);
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

  argv = optimist.boolean(["print_classpath", "print_command"]);

  args = _.clone(argv);

  if (args.sample != null) {
    if (args.properties != null) {
      console.error("Replacing provided properties with sample properties due to arg --sample");
    }
    args.properties = args.sample.indexOf("/") > -1 ? args.sample : "" + __dirname + "/" + args.sample;
  }

  if (args.print_classpath === "true") {
    kclhelper.getKclClasspath(function(cp) {
      console.log(cp);
    });
  } else if (args.print_command != null) {
    if ((args.java != null) && (args.properties != null)) {
      mld_class = "com.amazonaws.services.kinesis.multilang.MultiLangDaemon";
      paths = args.paths != null ? args.paths.split(",") : [];
      kclhelper.getKclAppCommand(args.java, mld_class, args.properties, paths);
    } else {
      console.error("Must provide arguments --java and --properties\n");
    }
  } else {
    usage = "--java  $path_to_java_executable - Required\n--props $rel_path_to_properties  - Required\n--print_classpath [false]        - Show classpath\n--print_command   [false]        - Show Java command\n--sample          [false]        - Use sample properties";
    console.error(usage);
  }

}).call(this);
