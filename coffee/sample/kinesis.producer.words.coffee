#!env /usr/local/bin/node

KCL = require '../kcl'
logger = require '../logger'

AWS = require 'aws-sdk'
async = require 'async'
logger = require '../logger'
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
		logger.info "sample_kclnode_app : KinesisProducer : getStreamStatus #{stream}"
		@kinesis.describeStream {StreamName: stream}, cb
		return
	waitForStream : (stream, cb) ->
		self = @
		streamStatus = null
		streamActivationPending = -> "ACTIVE" is streamStatus
		checkStream = (_cb) -> 
			if streamStatus?
				setTimeout ->
					self.getStreamStatus stream, _cb
				, self.sleepSeconds
			else
				self.getStreamStatus stream, _cb
			return
		async.doWhilst checkStream, streamActivationPending, cb
		return
	putWordsInStream : (stream, words=[], callback) ->
		logger.info "sample_kclnode_app : KinesisProducer : putWordsInStream #{words.length} #{stream}"
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
		logger.info "sample_kclnode_app : KinesisProducer : putWordsInStreamPeriodically"
		truthTest = -> true
		delay = (cb) ->
			putWordsInStream stream, words, ->
			setTimeout cb, period
			return
		callback = (err) ->
			logger.error "sample_kclnode_app : KinesisProducer : putWordsInStreamPeriodically", err
		async.whilst truthTest, delay, callback
		return


wordProducer = new KinesisWordProducer

argv = optimist.argv
usage = """
	--stream Stream Name                      - Required
	--region AWS Region                       - Required
	--words  Comma separated list of words    - Required
	--period Periodic put interval in seconds - Optional
"""

{stream, region, words, period} = argv

if stream? and region? and words?
	if words.indexOf(',')
		words = words.split(',')
	wordProducer.getStreamStatus(err, data) ->
		initiatePuts = ->
			console.log "Polling #{stream}:state"
			wordProducer.waitForStream stream, ->
				console.log "#{stream} is now ACTIVE"
				if period?
					wordProducer.putWordsInStreamPeriodically stream, words, period
				else
					wordProducer.putWordsInStream stream, words, ->
				return
		if err? or not data?
			# create
			wordProducer.createStream stream, (err, data) ->
				if err?
					console.log "Error creating stream [#{stream}]", err
				else
					do initiatePuts
				return
		if data? and data.StreamStatus?
			# bail
			if data.StreamStatus is "DELETING"
				console.log "Stream [#{stream}] is being deleted, please rerun the script later"
				process.exit(1)
		return
else
	console.error usage