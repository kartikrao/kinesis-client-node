#!env /usr/local/bin/node

optimist = require 'optimist'
_ = require 'lodash'
path = require 'path'
Glob = require('glob').Glob

class KCLHelper
	constructor : ->
		@kclpath = path.resolve("#{__dirname}/../")
		@separator = ":"
	getKclDir : -> @kclpath
	getKclJarPath : (cb) ->
		self = @
		new Glob "#{self.kclpath}/jars/**/*.jar", {},(err, files=[]) ->
			cb null, files.join(self.separator)
			return
		return
	getKclClasspath : (propertyPath=null, paths, cb) ->
		self = @

		userpaths = []
		# Handle User Supplied Paths
		if _.isString(paths) 
			if paths.indexOf(",") > -1
				paths = paths.split(",")
			else
				paths = [paths]
		else if _.isArray(paths)
			for p in paths
				userpaths.push(path.resolve(p)) if p?.length > 0

		@getKclJarPath (err, jarpaths) ->
			if propertyPath?
				propertiesFolder = propertyPath.substring(0, propertyPath.lastIndexOf("/") + 1)
				jarpaths = "#{jarpaths}:#{path.resolve(propertiesFolder)}"
			if userpaths?.length > 0
				jarpaths = userpaths.join(self.kclpath) + jarpaths
			cb null, jarpaths
		return
	getKclAppCommand : (java, mld_class, properties, paths=[], cb) ->
		baseName = properties
		if properties.indexOf("/") > -1
			baseName = properties.substring(properties.lastIndexOf("/") + 1)
		@getKclClasspath properties, paths, (err, cp) ->
			if not err?
				cb null, "#{java} -cp #{cp} #{mld_class} #{baseName}"
			else
				cb err
			return

kclhelper = new KCLHelper

argv = optimist.boolean(["print_classpath", "print_command"]).argv

if argv.sample?
	if argv.props?
		console.error "Replacing provided properties with sample properties due to arg --sample"
	argv.props = if argv.sample.indexOf("/") > -1 then argv.sample else "#{kclhelper.kclpath}/sample/#{argv.sample}"

if argv.print_classpath is true
	kclhelper.getKclClasspath argv.props, null, (err, cp) ->
		console.log "CLASSPATH=", cp
		return
else if argv.print_command?
	if argv.java? and argv.props?
		mld_class = "com.amazonaws.services.kinesis.multilang.MultiLangDaemon"
		paths = if argv.paths? then argv.paths.split(",") else []
		kclhelper.getKclAppCommand argv.java, mld_class, argv.props, paths, (err, command) ->
			console.log command
	else
		console.error "Must provide arguments --java and --props\n"
else
	usage = """
		--java  $path_to_java_executable - Required
		--props $rel_path_to_properties  - Required
		--print_classpath [false]        - Print classpath
		--print_command   [false]        - Print Java command
		--sample          [false]        - Use sample properties
	"""
	console.error usage



