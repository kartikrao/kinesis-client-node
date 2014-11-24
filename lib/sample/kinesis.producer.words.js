#!/usr/bin/env node
/**
 * KCL for node.js
 * @version   : 0.1.0
 * @copyright : kartik.rao@adinfinity.com.au
 * @license   : MIT
 **/
(function() {
  var AWS, KCL, KinesisWordProducer, argv, async, optimist, period, region, settings, stream, wordProducer, words;

  KCL = require('../kcl');

  AWS = require('aws-sdk');

  async = require('async');

  optimist = require('optimist');

  settings = require('./kinesis.app.words.settings');

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

    KinesisWordProducer.prototype.createStream = function(stream, cb) {
      this.kinesis.createStream({
        StreamName: stream,
        numShards: 1
      }, cb);
    };

    KinesisWordProducer.prototype.getStreamStatus = function(stream, cb) {
      console.log("KinesisProducer : getStreamStatus " + stream);
      this.kinesis.describeStream({
        StreamName: stream
      }, cb);
    };

    KinesisWordProducer.prototype.waitForStream = function(stream, cb) {
      var checkStream, self, statusCb, streamActivationPending, streamStatus;
      self = this;
      streamStatus = null;
      streamActivationPending = function() {
        return "ACTIVE" === streamStatus;
      };
      statusCb = function(err, status) {
        if (status != null) {
          streamStatus = status.StreamDescription.StreamStatus;
        }
      };
      checkStream = function(_cb) {
        if (streamStatus != null) {
          setTimeout(function() {
            return self.getStreamStatus(stream, statusCb);
          }, self.sleepSeconds);
        } else {
          self.getStreamStatus(stream, statusCb);
        }
      };
      async.doWhilst(checkStream, streamActivationPending, cb);
    };

    KinesisWordProducer.prototype.putWordsInStream = function(stream, words, callback) {
      var self, word, workers, _i, _len;
      if (words == null) {
        words = [];
      }
      console.log("KinesisProducer : putWordsInStream : Queueing [" + words.length + "] items to enter stream [" + stream + "]");
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
            console.log("" + (new Date().getTime()) + " - Kinesis.putRecord " + packet.Data);
            self.kinesis.putRecord(packet, cb);
          };
        })();
      }
      async.series(workers, callback);
    };

    KinesisWordProducer.prototype.putWordsInStreamPeriodically = function(stream, words, period) {
      var callback, delay, self, truthTest;
      self = this;
      if (period == null) {
        period = this.SLEEP_SECONDS;
      }
      console.log("KinesisProducer : putWordsInStreamPeriodically");
      truthTest = function() {
        return true;
      };
      delay = function(cb) {
        self.putWordsInStream(stream, words, function() {});
        setTimeout(cb, period);
      };
      callback = function(err) {
        console.log("KinesisProducer : putWordsInStreamPeriodically", err);
      };
      async.whilst(truthTest, delay, callback);
    };

    return KinesisWordProducer;

  })();

  wordProducer = new KinesisWordProducer;

  argv = optimist.argv;

  stream = argv.stream, region = argv.region, words = argv.words, period = argv.period;

  if ((stream != null) && (region != null) && (words != null)) {
    if (words.indexOf(',')) {
      words = words.split(',');
    }
    wordProducer.getStreamStatus(stream, function(err, status) {
      var initiatePut, streamStatus;
      initiatePut = function() {
        if (period != null) {
          wordProducer.putWordsInStreamPeriodically(stream, words, period, function() {});
        } else {
          wordProducer.putWordsInStream(stream, words, function() {
            console.log("KinesisProducer : " + words.length + " words added to stream [" + stream + "]");
          });
        }
      };
      if ((err != null) || (status == null)) {
        wordProducer.createStream(stream, function(err, data) {
          if (err != null) {
            console.log("KinesisProducer : Error creating stream [" + stream + "]", err);
          } else {
            initiatePut();
          }
        });
      }
      if (status != null) {
        streamStatus = status.StreamDescription.StreamStatus;
        console.log("KinesisProducer : stream [" + stream + "] is " + streamStatus);
        if (streamStatus === "DELETING") {
          console.log("KinesisProducer : stream [" + stream + "] is being deleted, please rerun the script later");
          process.exit(1);
        } else if (streamStatus === "ACTIVE") {
          console.log("KinesisProducer : stream [" + stream + "] is active, initiating put");
          initiatePut();
        } else {
          console.log("KinesisProducer : Waiting for stream [" + stream + "]");
          wordProducer.waitForStream(stream, initiatePut);
        }
      }
    });
  } else {
    console.error("--stream Stream Name                      - Required\n--region AWS Region                       - Required\n--words  Comma separated list of words    - Required\n--period Periodic put interval in seconds - Optional");
  }

}).call(this);
