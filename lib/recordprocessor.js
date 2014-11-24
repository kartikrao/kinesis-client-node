#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var EventEmitter, KCL, RecordProcesser, async, logger, timeMillis, _,
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  KCL = require('./kcl');

  EventEmitter = require('events').EventEmitter;

  async = require('async');

  timeMillis = function() {
    return Date.now();
  };

  logger = require('./logger');

  _ = require('underscore');

  RecordProcesser = (function(_super) {
    __extends(RecordProcesser, _super);

    function RecordProcesser(processer, SLEEP_SECONDS, CHECKPOINT_RETRIES, CHECKPOINT_FREQ_SECONDS) {
      this.processer = processer;
      this.SLEEP_SECONDS = SLEEP_SECONDS != null ? SLEEP_SECONDS : 5;
      this.CHECKPOINT_RETRIES = CHECKPOINT_RETRIES != null ? CHECKPOINT_RETRIES : 5;
      this.CHECKPOINT_FREQ_SECONDS = CHECKPOINT_FREQ_SECONDS != null ? CHECKPOINT_FREQ_SECONDS : 60;
    }

    RecordProcesser.prototype.run = function() {
      var self, stderr, stdin, stdout;
      self = this;
      stdin = process.stdin, stdout = process.stdout, stderr = process.stderr;
      this.kcl = new KCL(process, this);
      this.kcl.run();
    };

    RecordProcesser.prototype.processRecords = function(records, checkpointer, callback) {
      var agents, record, self, _i, _len;
      self = this;
      self.emit('records');
      agents = {};
      for (_i = 0, _len = records.length; _i < _len; _i++) {
        record = records[_i];
        agents[record.sequenceNumber] = (function() {
          var data, key, partitionKey, r, sequenceNumber;
          r = record;
          data = r.data, sequenceNumber = r.sequenceNumber, partitionKey = r.partitionKey;
          data = new Buffer(data, 'base64').toString('utf8');
          key = partitionKey;
          return function(cb) {
            self.processer.processRecord(data, sequenceNumber, key, function(err) {
              if (err != null) {
                logger.error(err);
              } else {
                if ((self.largest_seq == null) || self.largest_seq < sequenceNumber) {
                  self.largest_seq = sequenceNumber;
                }
                cb(null, {
                  sequenceNumber: sequenceNumber,
                  success: err == null,
                  error: err
                });
              }
            });
          };
        })();
      }
      async.series(agents, function(err, result) {
        if (err != null) {
          logger.error("RecordProcessor.processRecords - Error", err);
          callback(err);
        } else {
          callback(null, self.largest_seq);
        }
      });
    };

    RecordProcesser.prototype.shutdown = function(checkpointer, reason, callback) {
      self.emit('shutdown', reason);
      callback();
    };

    RecordProcesser.prototype.initialize = function(shard_id, callback) {
      this.shard_id = shard_id;
      this.emit('initialize', shard_id);
      this.largest_seq = null;
      this.last_checkpoint_time = 0;
      return callback();
    };

    return RecordProcesser;

  })(EventEmitter);

  module.exports = RecordProcesser;

}).call(this);
