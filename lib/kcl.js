#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var CheckpointError, Checkpointer, EventEmitter, IOHandler, KCL, byline, logger, readline, timeMillis, _,
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  readline = require('readline');

  _ = require('underscore');

  EventEmitter = require('events').EventEmitter;

  logger = require('./logger');

  byline = require('byline');

  timeMillis = function() {
    return Date.now();
  };

  IOHandler = (function() {
    function IOHandler() {
      var self;
      this.input_file = process.stdin;
      this.output_file = process.stdout;
      this.error_file = process.stderr;
      this.isTTY = process.stdin.isTTY;
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

    IOHandler.prototype.write_action = function(response) {
      logger.info("IOHandler : " + (new Date().getTime()) + " - Writing " + (JSON.stringify(response)));
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
    function Checkpointer(io_handler) {
      this.io_handler = io_handler;
    }

    Checkpointer.prototype.checkpoint = function(sequenceNumber, cb) {
      var lineHandler, response, self;
      if (sequenceNumber == null) {
        sequenceNumber = null;
      }
      response = {
        "action": "checkpoint",
        "checkpoint": sequenceNumber
      };
      self = this;
      lineHandler = function(line) {
        var data, error, ex;
        data = line;
        if (_.isString(line) === true) {
          data = JSON.parse(line);
        }
        logger.info("Checkpoint line listener " + (_.isFunction(lineHandler)) + " Line Action " + data.action);
        if ((data == null) || data.action !== "checkpoint") {
          error = (data != null ? data.error : void 0) || "InvalidStateException";
          logger.info("Checkpoint error " + error, data);
          cb(null, new Error(error));
        } else {
          cb(null);
        }
        if (lineHandler != null) {
          try {
            self.io_handler.lineReader.removeListener('line', lineHandler);
          } catch (_error) {
            ex = _error;
            logger.error("Listener FUCK UP", ex);
          }
        }
      };
      this.io_handler.lineReader.on('line', lineHandler);
      this.io_handler.write_action(response);
    };

    return Checkpointer;

  })();

  KCL = (function(_super) {
    __extends(KCL, _super);

    function KCL(_process, recordProcessor) {
      if (_process == null) {
        _process = process;
      }
      this.recordProcessor = recordProcessor;
      this.io_handler = new IOHandler(_process);
      this.checkpointer = new Checkpointer(this.io_handler);
    }

    KCL.prototype._perform_action = function(data) {
      var action, ensureKey, self;
      ensureKey = function(obj, key) {
        if (obj[key] == null) {
          throw new Error("Action " + obj.action + " was expected to have key " + key);
        }
        return obj[key];
      };
      action = data["action"];
      self = this;
      switch (action) {
        case "initialize":
          this.recordProcessor.initialize(ensureKey(data, "shardId"), function() {
            self._report_done('initialize');
          });
          break;
        case "processRecords":
          this.recordProcessor.processRecords(ensureKey(data, "records"), this.checkpointer, function(err, checkpointSeq) {
            self._report_done('processRecords');
            if (checkpointSeq != null) {
              self.checkpointer.checkpoint(self.checkpointer, checkpointSeq, function(err) {
                if (err) {
                  logger.error(err);
                }
              });
            }
            self.emit('checkpoint', checkpointSeq, timeMillis);
          });
          break;
        case 'shutdown':
          this.recordProcessor.shutdown(this.checkpointer, ensureKey(data, "reason"), function() {
            self._report_done('shutdown');
          });
          break;
        case 'checkpoint':
          logger.info("Checkpoint Data", data);
          logger.info('ignore checkpoint message');
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
    };

    KCL.prototype.run = function() {
      var self;
      self = this;
      this.io_handler.lineReader.on('line', function(line) {
        if ((line != null ? line.length : void 0) > 0) {
          logger.info("IOHandler : " + (new Date().getTime()) + " - Got Line " + (JSON.parse(line).action));
          self._handle_a_line(line);
        }
      });
    };

    return KCL;

  })(EventEmitter);

  module.exports = KCL;

}).call(this);
