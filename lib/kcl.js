/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var CheckpointError, Checkpointer, EventEmitter, IOHandler, KCL, helper, kcl, readline, _;

  readline = require('readline');

  _ = require('underscore');

  EventEmitter = require('events').EventEmitter;

  helper = require('./kclhelper');

  IOHandler = (function() {
    function IOHandler(_process) {
      var self;
      if (_process == null) {
        _process = process;
      }
      this.input_file = _process.stdin;
      this.output_file = _process.stdout;
      this.error_file = _process.stderr;
      this.isTTY = _process.stdin.isTTY;
      this.lineReader = readline.createInterface({
        input: this.input_file,
        output: this.output_file
      });
      self = this;
    }

    IOHandler.prototype.write_line = function(line) {
      this.output_file.write("" + line + "\n");
    };

    IOHandler.prototype.write_error = function(error) {
      this.error_file.write("" + error + "\n");
    };

    IOHandler.prototype.write_action = function() {
      this.write_line(JSON.stringify(response));
    };

    IOHandler.prototype.loadAction = function(line) {
      if ((line != null ? line.length : void 0) > 0) {
        return JSON.parse(line);
      } else {
        return null;
      }
    };

    return IOHandler;

  })();

  CheckpointError = (function() {
    CheckpointError.prototype.error = null;

    CheckpointError.prototype.message = "";

    function CheckpointError(errorStr) {
      this.message = errorStr;
      this.error = new Error(errorStr);
    }

    CheckpointError.prototype.value = function() {
      return this.error;
    };

    CheckpointError.prototype.asString = function() {
      return this.message;
    };

    return CheckpointError;

  })();

  Checkpointer = (function() {
    function Checkpointer(io_handler) {}

    Checkpointer.prototype.checkpoint = function(sequenceNumber, cb) {
      var listener, response, self;
      if (sequenceNumber == null) {
        sequenceNumber = null;
      }
      response = {
        "action": "checkpoint",
        "checkpoint": sequenceNumber
      };
      self = this;
      listener = this.io_handler.lineReader.on('line', function(line) {
        var data, error;
        data = line;
        if (_.isString(line) === true) {
          data = JSON.parse(line);
        }
        if ((data == null) || data.action !== "checkpoint") {
          error = (data != null ? data.error : void 0) || "InvalidStateException";
          cb(null, new Error(error));
        } else {
          cb(null);
        }
        self.io_handler.lineReader.removeListener('line', listener);
      });
      this.io_handler.write_action(response);
    };

    return Checkpointer;

  })();

  KCL = (function() {
    function KCL(_process) {
      if (_process == null) {
        _process = process;
      }
      this.io_handler = new IOHandler(_process);
      this.checkpointer = new Checkpointer(this.io_handler);
    }

    KCL.prototype._perform_action = function(data) {
      var action, ensureKey;
      ensureKey = function(obj, key) {
        if (obj[key] == null) {
          throw new Error("Action " + obj.action + " was expected to have key " + key);
        }
        return obj[key];
      };
      action = data["action"];
      switch (action) {
        case "initialize":
          this.emit('initialize', ensureKey(data, "shardId"));
          break;
        case "processRecords":
          this.emit('processRecords', ensureKey(data, "records"), this.checkpointer);
          break;
        case 'shutdown':
          this.emit('shutdown', checkpointer, ensureKey(data, "reason"));
          break;
        default:
          throw new Error("Received an action which couldn't be understood. Action was '" + action + "'");
      }
    };

    KCL.prototype._report_done = function(responseFor) {
      this.io_handler.write_action({
        "action": "status",
        "responseFor": responseFor
      });
    };

    KCL.prototype._handle_a_line = function(line) {
      var data;
      data = line;
      if (_.isString(line) === true) {
        data = JSON.parse(line);
      }
      this._perform_action(data);
      this._report_done(data.action);
    };

    KCL.prototype.run = function() {
      var self;
      self = this;
      this.io_handler.lineReader.on('line', function(line) {
        if ((line != null ? line.length : void 0) > 0) {
          self._handle_a_line(line);
        }
      });
    };

    return KCL;

  })();

  kcl = new KCL;

  kcl.run();

}).call(this);
