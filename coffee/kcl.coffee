#!env node

readline = require 'readline'
_ = require 'underscore'
EventEmitter = require('events').EventEmitter

class IOHandler
	# process.stdout and stderr are blocking in TTY mode
	constructor : (_process=process) ->
		@input_file = _process.stdin
		@output_file= _process.stdout
		@error_file = _process.stderr
		@isTTY      = _process.stdin.isTTY
		@lineReader = readline.createInterface {input: @input_file, output : @output_file}
		self = @
	write_line : (line) ->
		@output_file.write "#{line}\n"
		return
	write_error : (error) ->
		@error_file.write "#{error}\n"
		return
	write_action : (response) ->
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
	constructor : (io_handler) ->
	checkpoint : (sequenceNumber, cb) ->
		sequenceNumber ?= null
		response = {"action" : "checkpoint", "checkpoint" : sequenceNumber}
		self = @
		listener = @io_handler.lineReader.on 'line', (line) ->
			data = line
			if _.isString(line) is true
				data = JSON.parse(line)
			if not data? or data.action isnt "checkpoint"
				error = data?.error or "InvalidStateException"
				cb null, new Error(error)
			else
				cb null
			self.io_handler.lineReader.removeListener 'line', listener
			return
		@io_handler.write_action response
		return

class KCL extends EventEmitter
	constructor : (_process=process) ->
		@io_handler = new IOHandler _process
		@checkpointer = new Checkpointer @io_handler
	_perform_action : (data) ->
		ensureKey = (obj, key) ->
			unless obj[key]?
				throw new Error("Action #{obj.action} was expected to have key #{key}")
			return obj[key]
		action = data["action"]
		switch action
			when "initialize"
				@emit 'initialize', ensureKey(data, "shardId")
			when "processRecords"
				@emit 'processRecords', ensureKey(data, "records"), @checkpointer
			when 'shutdown'
				@emit 'shutdown', checkpointer, ensureKey(data, "reason")
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
		@_report_done data.action
		return
	run : ->
		self = @
		@io_handler.lineReader.on 'line', (line) ->
			if line?.length > 0
				self._handle_a_line line
			return
		return

kcl = new KCL
do kcl.run