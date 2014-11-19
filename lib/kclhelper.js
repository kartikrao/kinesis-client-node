#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var Glob, KCLHelper, argv, kclhelper, mld_class, optimist, path, paths, usage, _;

  optimist = require('optimist');

  _ = require('underscore');

  path = require('path');

  Glob = require('glob').Glob;

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
      new Glob("" + self.kclpath + "/jars/**/*.jar", {}, function(err, files) {
        if (files == null) {
          files = [];
        }
        cb(null, files.join(self.separator));
      });
    };

    KCLHelper.prototype.getKclClasspath = function(propertyPath, paths, cb) {
      var p, self, userpaths, _i, _len;
      if (propertyPath == null) {
        propertyPath = null;
      }
      self = this;
      userpaths = [];
      if (_.isString(paths)) {
        if (paths.indexOf(",") > -1) {
          paths = paths.split(",");
        } else {
          paths = [paths];
        }
      } else if (_.isArray(paths)) {
        for (_i = 0, _len = paths.length; _i < _len; _i++) {
          p = paths[_i];
          if ((p != null ? p.length : void 0) > 0) {
            userpaths.push(path.resolve(p));
          }
        }
      }
      this.getKclJarPath(function(err, jarpaths) {
        var propertiesFolder;
        if (propertyPath != null) {
          propertiesFolder = propertyPath.substring(0, propertyPath.lastIndexOf("/") + 1);
          jarpaths = "" + jarpaths + ":" + (path.resolve(propertiesFolder));
        }
        if ((userpaths != null ? userpaths.length : void 0) > 0) {
          jarpaths = userpaths.join(self.kclpath) + jarpaths;
        }
        return cb(null, jarpaths);
      });
    };

    KCLHelper.prototype.getKclAppCommand = function(java, mld_class, properties, paths, cb) {
      var baseName;
      if (paths == null) {
        paths = [];
      }
      baseName = properties;
      if (properties.indexOf("/") > -1) {
        baseName = properties.substring(properties.lastIndexOf("/") + 1);
      }
      return this.getKclClasspath(properties, paths, function(err, cp) {
        if (err == null) {
          cb(null, "" + java + " -cp " + cp + " " + mld_class + " " + baseName);
        } else {
          cb(err);
        }
      });
    };

    return KCLHelper;

  })();

  kclhelper = new KCLHelper;

  argv = optimist.boolean(["print_classpath", "print_command"]).argv;

  if (argv.sample != null) {
    if (argv.props != null) {
      console.error("Replacing provided properties with sample properties due to arg --sample");
    }
    argv.props = argv.sample.indexOf("/") > -1 ? argv.sample : "" + kclhelper.kclpath + "/sample/" + argv.sample;
  }

  if (argv.print_classpath === true) {
    kclhelper.getKclClasspath(argv.props, null, function(err, cp) {
      console.log("CLASSPATH=", cp);
    });
  } else if (argv.print_command != null) {
    if ((argv.java != null) && (argv.props != null)) {
      mld_class = "com.amazonaws.services.kinesis.multilang.MultiLangDaemon";
      paths = argv.paths != null ? argv.paths.split(",") : [];
      kclhelper.getKclAppCommand(argv.java, mld_class, argv.props, paths, function(err, command) {
        return console.log(command);
      });
    } else {
      console.error("Must provide arguments --java and --props\n");
    }
  } else {
    usage = "--java  $path_to_java_executable - Required\n--props $rel_path_to_properties  - Required\n--print_classpath [false]        - Print classpath\n--print_command   [false]        - Print Java command\n--sample          [false]        - Use sample properties";
    console.error(usage);
  }

}).call(this);
