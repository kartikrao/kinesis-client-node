#!env /usr/local/bin/node

optimist = require 'optimist'
_ = require 'underscore'
path = require 'path'
glob = require 'glob'

class KCLHelper
	constructor : ->
		@kclpath = path.resolve("#{__dirname}/../")
		@separator = ":"
	getKclDir : -> @kclpath
	getKclJarPath : (cb) ->
		self = @
		glob "#{self.kclpath}/jars/**/*.jar", (err, files=[]) ->
			cb files.join(self.separator)
			return
		return
	getKclClasspath : (propertyPath=null, paths=[], cb) ->
		rpaths = []
		rpaths.push path.resolve(p) for p in paths
		if propertyPath?.length > 0
			rpaths.push path.resolve(propertyPath)
		self = @
		@getKclJarPath (jarpath=[]) ->
			classpath = _.union rpaths, jarpath
			cb classpath.join(self.separator)
			return
		return
	getKclAppCommand : (java, mld_class, properties, paths=[]) ->
		baseName = properties
		if properties.indexOf("/") > -1
			baseName = properties.substring(properties.lastIndexOf("/") + 1)
		"#{java} -cp #{@getKclClasspath(properties, paths)} #{mld_class} #{baseName}"

kclhelper = new KCLHelper

argv = optimist.boolean(["print_classpath", "print_command"]).argv

if argv.sample?
	if argv.properties?
		console.error "Replacing provided properties with sample properties due to arg --sample"
	argv.properties = if argv.sample.indexOf("/") > -1 then argv.sample else "#{__dirname}/#{argv.sample}"

if argv.print_classpath is true
	kclhelper.getKclClasspath argv.properties, null, (cp) ->
		console.log cp
		return
else if argv.print_command?
	if argv.java? and argv.properties?
		mld_class = "com.amazonaws.services.kinesis.multilang.MultiLangDaemon"
		paths = if argv.paths? then argv.paths.split(",") else []
		kclhelper.getKclAppCommand argv.java, mld_class, argv.properties, paths
	else
		console.error "Must provide arguments --java and --properties\n"
else
	usage = """
		--java  $path_to_java_executable - Required
		--props $rel_path_to_properties  - Required
		--print_classpath [false]        - Show classpath
		--print_command   [false]        - Show Java command
		--sample          [false]        - Use sample properties
	"""
	console.error usage



