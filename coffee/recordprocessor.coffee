KCL = require './kcl'

EventEmitter = require('events').EventEmitter
async = require 'async'
timeMillis = -> Date.now()
logger = require './logger'
_ = require 'underscore'

class RecordProcesser extends EventEmitter
	constructor : (@processer, @SLEEP_SECONDS=5, @CHECKPOINT_RETRIES=5, @CHECKPOINT_FREQ_SECONDS=60) ->
	run : ->
		# Use spawned process's streams
		self = @
		{stdin, stdout, stderr} = process
		@kcl = new KCL(process, @)
		do @kcl.run
		return
	processRecords : (records, checkpointer, callback) ->
		self = @
		self.emit 'records'
		agents = {}
		for record in records
			agents[record.sequenceNumber] = do ->
				r = record
				{data, sequenceNumber, partitionKey} = r
				data = new Buffer(data, 'base64').toString('utf8')
				seq = parseInt sequenceNumber
				key = partitionKey
				(cb) ->
					self.processer.processRecord data, sequenceNumber, key, (err) ->
						if err?
							logger.error err
						else
							self.largest_seq = sequenceNumber if not self.largest_seq? or self.largest_seq < seq
							logger.info "processRecords : Processing complete, running callback"
							cb null, {sequenceNumber: sequenceNumber, success: !err?, error : err}
						return
					return
		async.series agents, (err, result) ->
			if err?
				logger.error "Waterfall error", err
				callback err
			else
				isCheckpoint = ((timeMillis() - self.last_checkpoint_time) / 1000) > self.CHECKPOINT_FREQ_SECONDS
				callback null, self.largest_seq
				logger.info "Waterfall result", result
			return
		return
	shutdown : (checkpointer, reason, callback) ->
		self.emit 'shutdown', reason
		if reason is 'TERMINATE'
			@checkpoint checkpointer, @largest_seq
		do callback
		return
	initialize : (shard_id, callback) ->
		@shard_id = shard_id
		@emit 'initialize', shard_id
		@largest_seq = null
		@last_checkpoint_time = 0
		do callback
	checkpoint : (checkpointer, sequenceNumber, callback) ->
		self = @
		n = 0
		checkpointer ?= self.kcl.checkpointer
		attempt = ->
			checkpointer.checkpoint sequenceNumber, (err) ->
				if err?
					switch err.toString()
						when 'ShutdownException'
							callback new Error('ShutdownException')
							return
						when 'ThrottlingException'
							if (self.CHECKPOINT_RETRIES - n) is 0
								callback new Error("CheckpointRetryLimit")
							return
						when 'InvalidStateException'
							callback new Error('InvalidStateException')
				else
					self.last_checkpoint_time = timeMillis()
					self.largest_seq = sequenceNumber
					do callback
			return
		do attempt
		return

module.exports = RecordProcesser