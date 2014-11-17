KCL = require './kcl'

EventEmitter = require('events').EventEmitter

timeMillis = -> Date.now()

class RecordProcesser extends EventEmitter
	constructor : (@processer, @SLEEP_SECONDS=5, @CHECKPOINT_RETRIES=5, @CHECKPOINT_FREQ_SECONDS=60) ->
	run : (child_process) ->
		# Use spawned process's streams
		self = @
		{stdin, stdout, stderr} = child_process
		kcl = new KCL(stdin, stdout, stderr)
		kcl.on 'initialize', ->
			self.initialize.apply self, arguments
			return
		kcl.on 'processRecords', ->
			self.processRecords.apply self, arguments
			return
		kcl.on 'shutdown', ->
			self.shutdown.apply self, arguments
			return
		@processer.on 'processed', (_seq) ->
			@largest_seq = _seq if not @largest_seq? or @largest_seq < _seq
			if ((timeMillis() - self.last_checkpoint_time) / 1000) > @CHECKPOINT_FREQ_SECONDS
				@checkpoint checkpointer, self.largest_seq + ""
				@last_checkpoint_time = timeMillis()
			return
		@processor.on 'error', (err) ->
			console.log err
			process.exit -1
			return
		return
	process_records : (records, checkpointer) ->
		self.emit 'records'
		for record in records
			data = new Buffer(record.data, 'base64').toString('utf8')
			seq = parseInt record["sequenceNumber"]
			key = record["partitionKey"]
			@processer.emit 'record', data, key, seq
		return
	shutdown : (checkpointer, reason) ->
		self.emit 'records', reason
		if reason is 'TERMINATE'
			@checkpoint checkpointer, @largest_seq
		return
	initialize : (shard_id) ->
		@shard_id = shard_id
		self.emit 'initialize', shard_id
		@largest_seq = null
		@last_checkpoint_time = hrtime
	checkpoint : (checkpointer, sequence_number=null) ->
		self = @
		n = 0
		attempt = ->
			checkpointer.checkpoint sequence_number, (err) ->
				if err?
					switch err.asString()
						when 'ShutdownException'
							self.emit 'checkpoint_error', err
							return
						when 'ThrottlingException'
							if (self.CHECKPOINT_RETRIES - n) is 0
								self.emit 'checkpoint_error', new Error("CheckpointRetryLimit")
							return
						when 'InvalidStateException'
							self.emit 'checkpoint_retry', err
						else
							self.emit 'checkpoint_retry', err
				else
					self.emit 'checkpoint'
			return
		do attempt
		return

module.exports = RecordProcesser