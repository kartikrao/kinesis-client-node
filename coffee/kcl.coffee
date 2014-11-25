#!env /usr/local/bin/node

readline = require 'readline'
_ = require 'underscore'
EventEmitter = require('events').EventEmitter
logger = require './logger'
async = require 'async'

timeMillis = -> Date.now()

class IOHandler
	# process.stdout and stderr are blocking in TTY mode
	constructor : (_process=process) ->
		@input_file = _process.stdin
		@output_file= _process.stdout
		@error_file = _process.stderr
		@isTTY      = _process.stdin.isTTY
		@lineReader = readline.createInterface {input: @input_file, output: @output_file}
		self = @
	writeLine : (line) ->
		@output_file.write "#{line}\n"
		return
	writeError : (error) ->
		@error_file.write "#{error}\n"
		return
	writeAction : (response) ->
		@writeLine JSON.stringify(response)
		return
	loadAction : (line) ->
		if line?.length > 0
			JSON.parse line
		else
			null

class KCL extends EventEmitter
	CHECKPOINT_RETRIES : 5
	CHECKPOINT_FREQ_SEC: 60
	SLEEP_MILLIS       : 5000
	lastCheckpointTime : 0
	checkpointSequence : null
	checkpointRetries  : {}
	largestSequence    : null
	constructor : (_process=process, @recordProcessor, config) ->
		@io_handler = new IOHandler _process
		if config?
			@SLEEP_MILLIS = parseInt(config["sleepSeconds"] or 5) * 1000
			@CHECKPOINT_RETRIES = parseInt(config["checkpointRetries"] or 5)
			@CHECKPOINT_FREQ_SEC= parseInt(config["checkpointFreqSeconds"] or 60)
	checkpoint : (sequenceNumber=null, cb) ->
		@checkpointRetries[sequenceNumber] ?= 0
		if @checkpointRetries[sequenceNumber] >= @CHECKPOINT_RETRIES
			throw new Error("CheckpointRetryLimit")
		@checkpointSequence = sequenceNumber
		@io_handler.writeAction {"action" : "checkpoint", "checkpoint" : sequenceNumber}
		@checkpointRetries[sequenceNumber] += 1
		do cb
		return
	performAction : (data) ->
		ensureKey = (obj, key) ->
			unless obj[key]?
				logger.error "KCL.performAction - Action #{obj.action} missing key #{key}"
				throw new Error("MissingKeyError")
			return obj[key]
		action = data["action"]
		self = @
		switch action
			when "initialize"
				logger.info "KCL.performAction - Initialize - SHARD #{data.shardId}"
				self.emit 'initialize', data
				@recordProcessor.initialize ensureKey(data, "shardId"), ->
					self.reportDone 'initialize'
					return
			when "processRecords"
				self.emit 'processRecords', data
				@recordProcessor.processRecords ensureKey(data, "records"), (err, sequenceNumber) ->
					# Handle err from recordProcessor
					self.reportDone 'processRecords'
					if self.largestSequence is null or self.largestSequence < sequenceNumber
						self.largestSequence = sequenceNumber
					# Checkpointing Logic
					needCheckpoint = ((timeMillis() - self.lastCheckpointTime) / 1000) > self.CHECKPOINT_FREQ_SEC
					if needCheckpoint is true and not (self.checkpointQueued is true and self.checkpointSequence == sequenceNumber)
						self.checkpoint sequenceNumber, ->
							logger.info "KCL.performAction - processRecords - Queue Checkpoint #{sequenceNumber}"
							return
					return
			when 'shutdown'
				self.emit 'shutdown', data
				logger.info "KCL.performAction - Shutdown"
				reason = ensureKey(data, "reason")
				@recordProcessor.shutdown ensureKey(data, "reason"), ->
					if reason is "TERMINATE"
						self.checkpoint self.largestSequence, ->
							self.reportDone 'shutdown'
							return
					return
			when 'checkpoint'
				if (data.error?.length > 4)
					switch data.error 
						when "ThrottlingException"
							setTimeout ->
								self.checkpoint sequenceNumber, ->
									logger.info "KCL.performAction - processRecords - Queue Checkpoint #{sequenceNumber}"
									return
							, SLEEP_MILLIS
						else
							error = new Error(data.error)
							logger.error "KCL.performAction - CheckpointError", error
							self.emit 'error', error
							throw error
				else
					self.emit 'checkpoint', self.largestSequence
					@checkpointQueued = false
					@lastCheckpointTime = timeMillis()
					logger.info "KCL.performAction - Checkpoint complete - #{self.largestSequence}"
					delete @checkpointRetries[@checkpointSequence]
					@checkpointSequence = null
			else
				error = new UnsupportedActionError()
				logger.error "Received an action which couldn't be understood. Action was '#{action}'", error
				self.emit 'error', error
				throw error
		return
	reportDone : (responseFor) ->
		@io_handler.writeAction {"action" : "status", "responseFor" : responseFor}
		return
	handleLine : (line) ->
		data = line
		if _.isString(line) is true
			data = JSON.parse(line)
		@performAction data
		return
	run : ->
		self = @
		@io_handler.lineReader.on 'line', (line) ->
			if line?.length > 0
				self.handleLine line
			return
		return

module.exports = KCL