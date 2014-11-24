#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var Checkpointer, EventEmitter, IOHandler, KCL, async, logger, readline, timeMillis, _,
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  readline = require('readline');

  _ = require('underscore');

  EventEmitter = require('events').EventEmitter;

  logger = require('./logger');

  async = require('async');

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

    IOHandler.prototype.writeLine = function(line) {
      this.output_file.write("" + line + "\n");
    };

    IOHandler.prototype.writeError = function(error) {
      this.error_file.write("" + error + "\n");
    };

    IOHandler.prototype.writeAction = function(response) {
      logger.info("IOHandler : " + (new Date().getTime()) + " - writeAction " + (JSON.stringify(response)));
      this.writeLine(JSON.stringify(response));
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

  Checkpointer = (function() {
    function Checkpointer(io_handler) {
      this.io_handler = io_handler;
    }

    Checkpointer.prototype.checkpoint = function(sequenceNumber, cb) {
      var response, self;
      if (sequenceNumber == null) {
        sequenceNumber = null;
      }
      response = {
        "action": "checkpoint",
        "checkpoint": sequenceNumber
      };
      self = this;
      this.io_handler.writeAction(response);
    };

    return Checkpointer;

  })();

  KCL = (function(_super) {
    __extends(KCL, _super);

    KCL.prototype.checkpointRetries = 5;

    KCL.prototype.checkpointFreqSeconds = 60;

    KCL.prototype.largestSequence = null;

    KCL.prototype.lastCheckpointTime = 0;

    KCL.prototype.checkpointSequence = null;

    function KCL(_process, recordProcessor, checkpointRetries, checkpointFreqSeconds) {
      if (_process == null) {
        _process = process;
      }
      this.recordProcessor = recordProcessor;
      this.io_handler = new IOHandler(_process);
      this.checkpointer = new Checkpointer(this.io_handler);
    }

    KCL.prototype.checkpoint = function(sequenceNumber, callback) {
      var attempt, attemptsRemaining, n, self;
      this.checkpointSequence = sequenceNumber;
      this.checkpointQueued = true;
      n = 0;
      self = this;
      attempt = function() {
        self.checkpointer.checkpoint(sequenceNumber, function(err) {
          if (err != null) {
            switch (err.toString()) {
              case 'ShutdownException':
                callback(new Error('ShutdownException'));
                break;
              case 'ThrottlingException':
                if ((self.checkpointRetries - n) === 0) {
                  callback(new Error("CheckpointRetryLimit"));
                } else {
                  n += 1;
                }
                break;
              case 'InvalidStateException':
                return callback(new Error('InvalidStateException'));
            }
          } else {
            return callback();
          }
        });
      };
      attemptsRemaining = function() {
        return n < self.checkpointRetries;
      };
      async.doWhilst(attempt, attemptsRemaining, callback);
    };

    KCL.prototype.performAction = function(data) {
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
            self.reportDone('initialize');
          });
          break;
        case "processRecords":
          this.recordProcessor.processRecords(ensureKey(data, "records"), this.checkpointer, function(err, sequenceNumber) {
            var needCheckpoint;
            self.reportDone('processRecords');
            logger.info("kcl processRecords " + sequenceNumber);
            if (self.largestSequence === null || self.largestSequence < sequenceNumber) {
              self.largestSequence = sequenceNumber;
            }
            needCheckpoint = ((timeMillis() - self.lastCheckpointTime) / 1000) > self.checkpointFreqSeconds;
            logger.info("kcl.processRecords Checkpoint " + needCheckpoint + " " + ((timeMillis() - self.lastCheckpointTime) / 1000));
            if (needCheckpoint === true && !(self.checkpointQueued && self.checkpointSequence === sequenceNumber)) {
              logger.info("Checkpoint Trigger " + sequenceNumber);
              self.checkpoint(sequenceNumber, function() {
                self.lastCheckpointTime = timeMillis();
                logger.info("Checkpoint Callback " + sequenceNumber);
              });
            }
          });
          break;
        case 'shutdown':
          this.recordProcessor.shutdown(this.checkpointer, ensureKey(data, "reason"), function() {
            self.reportDone('shutdown');
          });
          break;
        case 'checkpoint':
          this.checkpointQueued = false;
          logger.info('Checkpoint Action', data);
          if ((data.error != null) || data.checkpoint !== this.checkpointSequence) {
            throw new Error("Failed to checpoint - Error ['" + data.error + "']");
          }
          break;
        default:
          throw new Error("Received an action which couldn't be understood. Action was '" + action + "'");
      }
    };

    KCL.prototype.reportDone = function(responseFor) {
      this.io_handler.writeAction({
        "action": "status",
        "responseFor": responseFor
      });
      this.io_handler.lineReader.resume();
    };

    KCL.prototype.handleLine = function(line) {
      var data;
      data = line;
      if (_.isString(line) === true) {
        data = JSON.parse(line);
      }
      this.performAction(data);
    };

    KCL.prototype.run = function() {
      var self;
      self = this;
      this.io_handler.lineReader.on('line', function(line) {
        self.io_handler.lineReader.pause();
        if ((line != null ? line.length : void 0) > 0) {
          logger.info("IOHandler.stdin : " + (new Date().getTime()) + " - Line " + (JSON.parse(line).action));
          self.handleLine(line);
        }
      });
    };

    return KCL;

  })(EventEmitter);

  module.exports = KCL;

}).call(this);
