#!env /usr/local/bin/node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var EventEmitter, KCL, RecordProcesser, timeMillis,
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  KCL = require('./kcl');

  EventEmitter = require('events').EventEmitter;

  timeMillis = function() {
    return Date.now();
  };

  RecordProcesser = (function(_super) {
    __extends(RecordProcesser, _super);

    function RecordProcesser(processer, SLEEP_SECONDS, CHECKPOINT_RETRIES, CHECKPOINT_FREQ_SECONDS) {
      this.processer = processer;
      this.SLEEP_SECONDS = SLEEP_SECONDS != null ? SLEEP_SECONDS : 5;
      this.CHECKPOINT_RETRIES = CHECKPOINT_RETRIES != null ? CHECKPOINT_RETRIES : 5;
      this.CHECKPOINT_FREQ_SECONDS = CHECKPOINT_FREQ_SECONDS != null ? CHECKPOINT_FREQ_SECONDS : 60;
    }

    RecordProcesser.prototype.run = function(child_process) {
      var kcl, self, stderr, stdin, stdout;
      self = this;
      stdin = child_process.stdin, stdout = child_process.stdout, stderr = child_process.stderr;
      kcl = new KCL(stdin, stdout, stderr);
      kcl.on('initialize', function() {
        self.initialize.apply(self, arguments);
      });
      kcl.on('processRecords', function() {
        self.processRecords.apply(self, arguments);
      });
      kcl.on('shutdown', function() {
        self.shutdown.apply(self, arguments);
      });
      this.processer.on('processed', function(_seq) {
        if ((this.largest_seq == null) || this.largest_seq < _seq) {
          this.largest_seq = _seq;
        }
        if (((timeMillis() - self.last_checkpoint_time) / 1000) > this.CHECKPOINT_FREQ_SECONDS) {
          this.checkpoint(checkpointer, self.largest_seq + "");
          this.last_checkpoint_time = timeMillis();
        }
      });
      this.processor.on('error', function(err) {
        console.log(err);
        process.exit(-1);
      });
    };

    RecordProcesser.prototype.process_records = function(records, checkpointer) {
      var data, key, record, seq, _i, _len;
      self.emit('records');
      for (_i = 0, _len = records.length; _i < _len; _i++) {
        record = records[_i];
        data = new Buffer(record.data, 'base64').toString('utf8');
        seq = parseInt(record["sequenceNumber"]);
        key = record["partitionKey"];
        this.processer.emit('record', data, key, seq);
      }
    };

    RecordProcesser.prototype.shutdown = function(checkpointer, reason) {
      self.emit('records', reason);
      if (reason === 'TERMINATE') {
        this.checkpoint(checkpointer, this.largest_seq);
      }
    };

    RecordProcesser.prototype.initialize = function(shard_id) {
      this.shard_id = shard_id;
      self.emit('initialize', shard_id);
      this.largest_seq = null;
      return this.last_checkpoint_time = hrtime;
    };

    RecordProcesser.prototype.checkpoint = function(checkpointer, sequence_number) {
      var attempt, n, self;
      if (sequence_number == null) {
        sequence_number = null;
      }
      self = this;
      n = 0;
      attempt = function() {
        checkpointer.checkpoint(sequence_number, function(err) {
          if (err != null) {
            switch (err.asString()) {
              case 'ShutdownException':
                self.emit('checkpoint_error', err);
                break;
              case 'ThrottlingException':
                if ((self.CHECKPOINT_RETRIES - n) === 0) {
                  self.emit('checkpoint_error', new Error("CheckpointRetryLimit"));
                }
                break;
              case 'InvalidStateException':
                return self.emit('checkpoint_retry', err);
              default:
                return self.emit('checkpoint_retry', err);
            }
          } else {
            return self.emit('checkpoint');
          }
        });
      };
      attempt();
    };

    return RecordProcesser;

  })(EventEmitter);

  module.exports = RecordProcesser;

}).call(this);
