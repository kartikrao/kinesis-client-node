#!env /usr/local/bin/node

readline = require 'readline'
_ = require 'underscore'
EventEmitter = require('events').EventEmitter
logger = require './logger'
byline = require 'byline'
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
	write_line : (line) ->
		@output_file.write "#{line}\n"
		return
	write_error : (error) ->
		@error_file.write "#{error}\n"
		return
	write_action : (response) ->
		logger.info "IOHandler : #{new Date().getTime()} - Writing #{JSON.stringify(response)}"
		@write_line JSON.stringify(response)
		return
	loadAction : (line) ->
		if line?.length > 0
			JSON.parse line
		else
			null

class CheckpointError
	error : null
	message : ""
	constructor : (errorStr) ->
		@message = errorStr
		@error = new Error(errorStr)
	value : -> @error
	asString : -> @message

class Checkpointer
	constructor : (@io_handler) ->
	checkpoint : (sequenceNumber, cb) ->
		sequenceNumber ?= null
		response = {"action" : "checkpoint", "checkpoint" : sequenceNumber}
		self = @
		lineHandler = (line) ->
			data = line
			if _.isString(line) is true
				data = JSON.parse(line)
			logger.info "Checkpoint line listener #{_.isFunction(lineHandler)} Line Action #{data.action}" 
			if not data? or data.action isnt "checkpoint"
				error = data?.error or "InvalidStateException"
				logger.info "Checkpoint error #{error}", data 
				cb null, new Error(error)
			else
				cb null
			if lineHandler?
				try
					self.io_handler.lineReader.removeListener 'line', lineHandler
				catch ex
					logger.error "Listener FUCK UP", ex
			return
		@io_handler.lineReader.on 'line', lineHandler
		@io_handler.write_action response
		return

class KCL extends EventEmitter
	constructor : (_process=process, @recordProcessor) ->
		@io_handler = new IOHandler _process
		@checkpointer = new Checkpointer @io_handler
	_perform_action : (data) ->
		ensureKey = (obj, key) ->
			unless obj[key]?
				throw new Error("Action #{obj.action} was expected to have key #{key}")
			return obj[key]
		action = data["action"]
		self = @
		switch action
			when "initialize"
				@recordProcessor.initialize ensureKey(data, "shardId"), ->
					self._report_done 'initialize'
					return
			when "processRecords"
				@recordProcessor.processRecords ensureKey(data, "records"), @checkpointer, (err, checkpointSeq) ->
					self._report_done 'processRecords'
					if checkpointSeq?
						self.checkpointer.checkpoint self.checkpointer, checkpointSeq, (err) ->
							logger.error(err) if err
							return
					self.emit 'checkpoint', checkpointSeq, timeMillis
					return
			when 'shutdown'
				@recordProcessor.shutdown @checkpointer, ensureKey(data, "reason"), ->
					self._report_done 'shutdown'
					return
			when 'checkpoint'
				logger.info "Checkpoint Data", data
				logger.info 'ignore checkpoint message'
			else
				throw new Error("Received an action which couldn't be understood. Action was '#{action}'")
		# handle @@mit throws error
		return
	_report_done : (responseFor) ->
		@io_handler.write_action {"action" : "status", "responseFor" : responseFor}
		return
	_handle_a_line : (line) ->
		data = line
		if _.isString(line) is true
			data = JSON.parse(line)
		@_perform_action data
		return
	run : ->
		self = @
		@io_handler.lineReader.on 'line', (line) ->
			if line?.length > 0
				logger.info "IOHandler : #{new Date().getTime()} - Got Line #{JSON.parse(line).action}"
				self._handle_a_line line
			return
		return

module.exports = KCL