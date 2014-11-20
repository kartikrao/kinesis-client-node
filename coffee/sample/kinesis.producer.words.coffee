#!env /usr/local/bin/node

KCL = require '../kcl'
AWS = require 'aws-sdk'
async = require 'async'
optimist = require 'optimist'
settings = require './kinesis.app.words.settings'

class KinesisWordProducer
	constructor : ->
		@kinesis = new AWS.Kinesis settings.aws
		rpSettings = settings.app.recordProcessor
		@StreamName = rpSettings.streamName
		@SLEEP_SECONDS = rpSettings.sleepSeconds
		@CHECKPOINT_RETRIES = rpSettings.checkpointRetries
		@CHECKPOINT_FREQ_SECONDS = rpSettings.checkpointFreqSeconds
	createStream    : (stream, cb) ->
		@kinesis.createStream {StreamName: stream, numShards : 1}, cb
		return
	getStreamStatus : (stream, cb) ->
		console.log "KinesisProducer : getStreamStatus #{stream}"
		@kinesis.describeStream {StreamName: stream}, cb
		return
	waitForStream : (stream, cb) ->
		self = @
		streamStatus = null
		streamActivationPending = -> "ACTIVE" is streamStatus
		statusCb = (err, status) ->
			if status?
				streamStatus = status.StreamDescription.StreamStatus
			return
		checkStream = (_cb) -> 
			if streamStatus?
				setTimeout ->
					self.getStreamStatus stream, statusCb
				, self.sleepSeconds
			else
				self.getStreamStatus stream, statusCb
			return
		async.doWhilst checkStream, streamActivationPending, cb
		return
	putWordsInStream : (stream, words=[], callback) ->
		console.log "KinesisProducer : putWordsInStream : Queueing [#{words.length}] items to enter stream [#{stream}]"
		self = @
		workers = {}
		for word in words
			workers[word] = do ->
				w = word
				packet =
					Data : new Buffer(w, 'utf8')
					PartitionKey : w
					StreamName   : stream
				(cb) -> self.kinesis.putRecord packet, cb
		async.series workers, callback
		return
	putWordsInStreamPeriodically : (stream, words, period) ->
		self = @
		period ?= @SLEEP_SECONDS
		console.log "KinesisProducer : putWordsInStreamPeriodically"
		truthTest = -> true
		delay = (cb) ->
			putWordsInStream stream, words, ->
			setTimeout cb, period
			return
		callback = (err) ->
			console.log "KinesisProducer : putWordsInStreamPeriodically", err
			return
		async.whilst truthTest, delay, callback
		return

wordProducer = new KinesisWordProducer

argv = optimist.argv

{stream, region, words, period} = argv

if stream? and region? and words?
	if words.indexOf(',')
		words = words.split(',')
	wordProducer.getStreamStatus stream, (err, status) ->
		initiatePut = ->
			if period?
				wordProducer.putWordsInStreamPeriodically stream, words, period, ->
			else
				wordProducer.putWordsInStream stream, words, ->
					console.log "KinesisProducer : #{words.length} words added to stream [#{stream}]"
					return
			return
		if err? or not status?
			# Create Stream
			wordProducer.createStream stream, (err, data) ->
				if err?
					console.log "KinesisProducer : Error creating stream [#{stream}]", err
				else
					do initiatePut
				return
		if status?
			streamStatus = status.StreamDescription.StreamStatus
			# Abort
			console.log "KinesisProducer : stream [#{stream}] is #{streamStatus}"

			if streamStatus is "DELETING"
				console.log "KinesisProducer : stream [#{stream}] is being deleted, please rerun the script later"
				process.exit(1)
			else if streamStatus is "ACTIVE"
				console.log "KinesisProducer : stream [#{stream}] is active, initiating put"
				do initiatePut
			else
				console.log "KinesisProducer : Waiting for stream [#{stream}]"
				wordProducer.waitForStream stream, initiatePut
		return
else
	console.error """
	--stream Stream Name                      - Required
	--region AWS Region                       - Required
	--words  Comma separated list of words    - Required
	--period Periodic put interval in seconds - Optional
"""