#!env /usr/local/bin/node

readline = require 'readline'
_ = require 'underscore'
EventEmitter = require('events').EventEmitter
logger = require './logger'
async = require 'async'

timeMillis = -> Date.now()

class IOHandler
	# process.stdout and stderr are blocking in TTY mode
	constructor : ->
		@input_file = process.stdin
		@output_file= process.stdout
		@error_file = process.stderr
		@isTTY      = process.stdin.isTTY
		@lineReader = readline.createInterface {input: @input_file, output: @output_file}
		self = @
	writeLine : (line) ->
		logger.info "Write Line Response", @output_file.write "#{line}\n"
		return
	writeError : (error) ->
		@error_file.write "#{error}\n"
		return
	writeAction : (response) ->
		logger.info "IOHandler : #{new Date().getTime()} - writeAction #{JSON.stringify(response)}"
		@writeLine JSON.stringify(response)
		return
	loadAction : (line) ->
		if line?.length > 0
			JSON.parse line
		else
			null

class Checkpointer
	constructor : (@io_handler) ->
	checkpoint : (sequenceNumber=null, cb) ->
		response = {"action" : "checkpoint", "checkpoint" : sequenceNumber}
		@io_handler.writeAction response
		process.nextTick ->
			do cb
		return

class KCL extends EventEmitter
	defaultCheckpointRetries : 5
	checkpointRetries : {}
	checkpointFreqSeconds : 60
	largestSequence : null
	lastCheckpointTime : 0
	checkpointSequence : null
	constructor : (_process=process, @recordProcessor, checkpointRetries, checkpointFreqSeconds) ->
		@io_handler = new IOHandler _process
		@checkpointer = new Checkpointer @io_handler
	checkpoint : (sequenceNumber, callback) ->
		@checkpointSequence = sequenceNumber
		@checkpointQueued = true
		n = 1
		self = @
		attempt = (cb) ->
			logger.info "Checkpoint attempt #{n}"
			self.checkpointer.checkpoint sequenceNumber, (err) ->
				if err?
					switch err.toString()
						when 'ShutdownException'
							cb new Error('ShutdownException')
						when 'ThrottlingException'
							if (self.checkpointRetries - n) is 0
								cb new Error("CheckpointRetryLimit")
						when 'InvalidStateException'
							cb new Error('InvalidStateException')
				do cb
			return
		attemptsRemaining = ->
			n += 1
			n <= self.checkpointRetries
		async.doWhilst attempt, attemptsRemaining, callback
		return
	performAction : (data) ->
		ensureKey = (obj, key) ->
			unless obj[key]?
				throw new Error("Action #{obj.action} was expected to have key #{key}")
			return obj[key]
		action = data["action"]
		self = @
		switch action
			when "initialize"
				@recordProcessor.initialize ensureKey(data, "shardId"), ->
					self.reportDone 'initialize'
					return
			when "processRecords"
				@recordProcessor.processRecords ensureKey(data, "records"), @checkpointer, (err, sequenceNumber) ->
					self.reportDone 'processRecords'
					logger.info "kcl processRecords #{sequenceNumber}"
					if self.largestSequence is null or self.largestSequence < sequenceNumber
						self.largestSequence = sequenceNumber
					# Checkpointing Logic
					needCheckpoint = ((timeMillis() - self.lastCheckpointTime) / 1000) > self.checkpointFreqSeconds
					logger.info "kcl.processRecords Checkpoint #{needCheckpoint} #{(timeMillis() - self.lastCheckpointTime) / 1000}"
					if needCheckpoint is true and not (self.checkpointQueued and self.checkpointSequence is sequenceNumber)
						self.checkpointRetries[sequenceNumber] ?= 0
						if self.checkpointRetries[sequenceNumber] >= self.defaultCheckpointRetries
							throw new Error("CheckpointRetryLimit")
						self.checkpointer.checkpoint sequenceNumber, ->
							self.checkpointRetries[sequenceNumber] += 1
					return
			when 'shutdown'
				@recordProcessor.shutdown @checkpointer, ensureKey(data, "reason"), ->
					self.reportDone 'shutdown'
					return
			when 'checkpoint'
				logger.info 'Checkpoint Action', data
				if (data.error? and !(data.error == 'null')) or data.checkpoint isnt @checkpointSequence
					logger.error "CheckpointError", data.error
					throw new Error(data.error)
				else
					@checkpointQueued = false
					@lastCheckpointTime = timeMillis()
					logger.info "Checkpoint completed - #{checkpointSequence} after #{self.checkpointRetries[checkpointSequence]} tries"
					delete @checkpointRetries[@checkpointSequence]
					@checkpointSequence = null
			else
				throw new Error("Received an action which couldn't be understood. Action was '#{action}'")
		return
	reportDone : (responseFor) ->
		@io_handler.writeAction {"action" : "status", "responseFor" : responseFor}
		#do @io_handler.lineReader.resume
		return
	handleLine : (line) ->
		data = line
		if _.isString(line) is true
			data = JSON.parse(line)
		logger.info "IOHandler.stdin : #{new Date().getTime()} - Line #{data.action} - #{data.error}"
		@performAction data
		return
	run : ->
		self = @
		@io_handler.lineReader.on 'line', (line) ->
			#do self.io_handler.lineReader.pause
			if line?.length > 0
				self.handleLine line
			return
		return

module.exports = KCL