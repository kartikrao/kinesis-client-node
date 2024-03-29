#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var EventEmitter, IOHandler, KCL, async, logger, readline, timeMillis, _,
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  readline = require('readline');

  _ = require('lodash');

  EventEmitter = require('events').EventEmitter;

  logger = require('./logger');

  async = require('async');

  timeMillis = function() {
    return Date.now();
  };

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

    IOHandler.prototype.writeLine = function(line) {
      this.output_file.write("" + line + "\n");
    };

    IOHandler.prototype.writeError = function(error) {
      this.error_file.write("" + error + "\n");
    };

    IOHandler.prototype.writeAction = function(response) {
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

  KCL = (function(_super) {
    __extends(KCL, _super);

    KCL.prototype.CHECKPOINT_RETRIES = 5;

    KCL.prototype.CHECKPOINT_FREQ_SEC = 60;

    KCL.prototype.SLEEP_MILLIS = 5000;

    KCL.prototype.lastCheckpointTime = 0;

    KCL.prototype.checkpointSequence = null;

    KCL.prototype.checkpointRetries = {};

    KCL.prototype.largestSequence = null;

    function KCL(_process, recordProcessor, config) {
      if (_process == null) {
        _process = process;
      }
      this.recordProcessor = recordProcessor;
      this.io_handler = new IOHandler(_process);
      if (config != null) {
        this.SLEEP_MILLIS = parseInt(config["sleepSeconds"] || 5) * 1000;
        this.CHECKPOINT_RETRIES = parseInt(config["checkpointRetries"] || 5);
        this.CHECKPOINT_FREQ_SEC = parseInt(config["checkpointFreqSeconds"] || 60);
      }
    }

    KCL.prototype.checkpoint = function(sequenceNumber, cb) {
      var _base;
      if (sequenceNumber == null) {
        sequenceNumber = null;
      }
      if ((_base = this.checkpointRetries)[sequenceNumber] == null) {
        _base[sequenceNumber] = 0;
      }
      if (this.checkpointRetries[sequenceNumber] >= this.CHECKPOINT_RETRIES) {
        throw new Error("CheckpointRetryLimit");
      }
      this.checkpointSequence = sequenceNumber;
      this.io_handler.writeAction({
        "action": "checkpoint",
        "checkpoint": sequenceNumber
      });
      this.checkpointRetries[sequenceNumber] += 1;
      cb();
    };

    KCL.prototype.performAction = function(data) {
      var action, ensureKey, error, reason, self, _ref;
      ensureKey = function(obj, key) {
        if (obj[key] == null) {
          logger.error("KCL.performAction - Action " + obj.action + " missing key " + key);
          throw new Error("MissingKeyError");
        }
        return obj[key];
      };
      action = data["action"];
      self = this;
      switch (action) {
        case "initialize":
          logger.info("KCL.performAction - Initialize - SHARD " + data.shardId);
          self.emit('initialize', data);
          this.recordProcessor.initialize(ensureKey(data, "shardId"), function() {
            self.reportDone('initialize');
          });
          break;
        case "processRecords":
          self.emit('processRecords', data);
          this.recordProcessor.processRecords(ensureKey(data, "records"), function(err, sequenceNumber) {
            var needCheckpoint;
            self.reportDone('processRecords');
            if (self.largestSequence === null || self.largestSequence < sequenceNumber) {
              self.largestSequence = sequenceNumber;
            }
            needCheckpoint = ((timeMillis() - self.lastCheckpointTime) / 1000) > self.CHECKPOINT_FREQ_SEC;
            if (needCheckpoint === true && !(self.checkpointQueued === true && self.checkpointSequence === sequenceNumber)) {
              self.checkpoint(sequenceNumber, function() {
                logger.info("KCL.performAction - processRecords - Queue Checkpoint " + sequenceNumber);
              });
            }
          });
          break;
        case 'shutdown':
          self.emit('shutdown', data);
          logger.info("KCL.performAction - Shutdown");
          reason = ensureKey(data, "reason");
          this.recordProcessor.shutdown(ensureKey(data, "reason"), function() {
            if (reason === "TERMINATE") {
              self.checkpoint(self.largestSequence, function() {
                self.reportDone('shutdown');
              });
            }
          });
          break;
        case 'checkpoint':
          if (((_ref = data.error) != null ? _ref.length : void 0) > 4) {
            switch (data.error) {
              case "ThrottlingException":
                setTimeout(function() {
                  return self.checkpoint(sequenceNumber, function() {
                    logger.info("KCL.performAction - processRecords - Queue Checkpoint " + sequenceNumber);
                  });
                }, SLEEP_MILLIS);
                break;
              default:
                error = new Error(data.error);
                logger.error("KCL.performAction - CheckpointError", error);
                self.emit('error', error);
                throw error;
            }
          } else {
            self.emit('checkpoint', self.largestSequence);
            this.checkpointQueued = false;
            this.lastCheckpointTime = timeMillis();
            logger.info("KCL.performAction - Checkpoint complete - " + self.largestSequence);
            delete this.checkpointRetries[this.checkpointSequence];
            this.checkpointSequence = null;
          }
          break;
        default:
          error = new UnsupportedActionError();
          logger.error("Received an action which couldn't be understood. Action was '" + action + "'", error);
          self.emit('error', error);
          throw error;
      }
    };

    KCL.prototype.reportDone = function(responseFor) {
      this.io_handler.writeAction({
        "action": "status",
        "responseFor": responseFor
      });
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
        if ((line != null ? line.length : void 0) > 0) {
          self.handleLine(line);
        }
      });
    };

    return KCL;

  })(EventEmitter);

  module.exports = KCL;

}).call(this);
