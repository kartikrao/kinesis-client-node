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
				key = partitionKey
				(cb) ->
					self.processer.processRecord data, sequenceNumber, key, (err) ->
						if err?
							logger.error err
						else
							self.largest_seq = sequenceNumber if not self.largest_seq? or self.largest_seq < sequenceNumber
							cb null, {sequenceNumber: sequenceNumber, success: !err?, error : err}
						return
					return
		async.series agents, (err, result) ->
			if err?
				logger.error "RecordProcessor.processRecords - Error", err
				callback err
			else
				callback null, self.largest_seq
			return
		return
	shutdown : (checkpointer, reason, callback) ->
		self.emit 'shutdown', reason
		do callback
		return
	initialize : (shard_id, callback) ->
		@shard_id = shard_id
		@emit 'initialize', shard_id
		@largest_seq = null
		@last_checkpoint_time = 0
		do callback

module.exports = RecordProcesser