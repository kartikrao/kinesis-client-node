KCL = require './kcl'

EventEmitter = require('events').EventEmitter
async = require 'async'
timeMillis = -> Date.now()
logger = require './logger'
_ = require 'lodash'

class RecordProcesser
	constructor : (@processer) ->
		unless @processer? and _.isFunction(@processer.processRecords)
			throw new Error("ProcessorInterfaceIncompatible")
	run : ->
		# Use spawned process's streams
		self = @
		{stdin, stdout, stderr} = process
		@kcl = new KCL(process, @)
		do @kcl.run
		return
	processRecords : (records, callback) ->
		self = @
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
	shutdown : (reason, callback) ->
		if @processer.shutdown?
			@processer.shutdown reason, (err) ->
				callback err
				return
		else
			do callback
		return
	initialize : (shard_id, callback) ->
		@shard_id = shard_id
		@largest_seq = null
		if @processer.initialize?
			@processer.initialize shard_id, (err) ->
				callback err
				return
			return
		do callback
		return

module.exports = RecordProcesser