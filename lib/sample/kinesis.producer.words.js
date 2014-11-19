#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var AWS, KCL, KinesisWordProducer, async, logger, settings;

  KCL = require('../kcl');

  logger = require('../logger');

  AWS = require('aws-sdk');

  settings = require('./sample_kclnode.settings');

  async = require('async');

  logger = require('../logger');

  KinesisWordProducer = (function() {
    function KinesisWordProducer() {
      var rpSettings;
      this.kinesis = new AWS.Kinesis(settings.aws);
      rpSettings = settings.app.recordProcessor;
      this.StreamName = rpSettings.streamName;
      this.SLEEP_SECONDS = rpSettings.sleepSeconds;
      this.CHECKPOINT_RETRIES = rpSettings.checkpointRetries;
      this.CHECKPOINT_FREQ_SECONDS = rpSettings.checkpointFreqSeconds;
    }

    KinesisWordProducer.prototype.getStreamStatus = function(stream, cb) {
      logger.info("sample_kclnode_app : KinesisProducer : getStreamStatus " + stream);
      this.kinesis.describeStream({
        StreamName: stream
      }, cb);
    };

    KinesisWordProducer.prototype.waitForStream = function(stream, cb) {
      var checkStream, self, streamActivationPending, streamStatus;
      self = this;
      streamStatus = null;
      streamActivationPending = function() {
        return "ACTIVE" === streamStatus;
      };
      checkStream = function(_cb) {
        if (streamStatus != null) {
          setTimeout(function() {
            return self.getStreamStatus(stream, _cb);
          }, self.sleepSeconds);
        } else {
          self.getStreamStatus(stream, _cb);
        }
      };
      async.doWhilst(checkStream, streamActivationPending, cb);
    };

    KinesisWordProducer.prototype.putWordsInStream = function(stream, words, callback) {
      var self, word, workers, _i, _len;
      if (words == null) {
        words = [];
      }
      logger.info("sample_kclnode_app : KinesisProducer : putWordsInStream " + words.length + " " + stream);
      self = this;
      workers = {};
      for (_i = 0, _len = words.length; _i < _len; _i++) {
        word = words[_i];
        workers[word] = (function() {
          var packet, w;
          w = word;
          packet = {
            Data: new Buffer(w, 'utf8'),
            PartitionKey: w,
            StreamName: stream
          };
          return function(cb) {
            return self.kinesis.putRecord(packet, cb);
          };
        })();
      }
      async.series(workers, callback);
    };

    KinesisWordProducer.prototype.putWordsInStreamPeriodically = function(stream, words) {
      var callback, delay, self, truthTest;
      self = this;
      logger.info("sample_kclnode_app : KinesisProducer : putWordsInStreamPeriodically");
      truthTest = function() {
        return true;
      };
      delay = function(cb) {
        putWordsInStream(stream, words, function() {});
        setTimeout(cb, self.SLEEP_SECONDS);
      };
      callback = function(err) {
        return logger.error("sample_kclnode_app : KinesisProducer : putWordsInStreamPeriodically", err);
      };
      async.whilst(truthTest, delay, callback);
    };

    return KinesisWordProducer;

  })();

  module.exports = KinesisWordProducer;

}).call(this);
