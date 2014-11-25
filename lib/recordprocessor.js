#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var EventEmitter, KCL, RecordProcesser, async, logger, timeMillis, _;

  KCL = require('./kcl');

  EventEmitter = require('events').EventEmitter;

  async = require('async');

  timeMillis = function() {
    return Date.now();
  };

  logger = require('./logger');

  _ = require('lodash');

  RecordProcesser = (function() {
    function RecordProcesser(processer) {
      this.processer = processer;
      if (!((this.processer != null) && _.isFunction(this.processer.processRecords))) {
        throw new Error("ProcessorInterfaceIncompatible");
      }
    }

    RecordProcesser.prototype.run = function() {
      var self, stderr, stdin, stdout;
      self = this;
      stdin = process.stdin, stdout = process.stdout, stderr = process.stderr;
      this.kcl = new KCL(process, this);
      this.kcl.run();
    };

    RecordProcesser.prototype.processRecords = function(records, callback) {
      var agents, record, self, _i, _len;
      self = this;
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

    RecordProcesser.prototype.shutdown = function(reason, callback) {
      if (this.processer.shutdown != null) {
        this.processer.shutdown(reason, function(err) {
          callback(err);
        });
      } else {
        callback();
      }
    };

    RecordProcesser.prototype.initialize = function(shard_id, callback) {
      this.shard_id = shard_id;
      this.largest_seq = null;
      if (this.processer.initialize != null) {
        this.processer.initialize(shard_id, function(err) {
          callback(err);
        });
        return;
      }
      callback();
    };

    return RecordProcesser;

  })();

  module.exports = RecordProcesser;

}).call(this);
