settings = {
	"aws" : {
		"region"  : "ap-souteast-2",
		"accessKeyId" : "AKIAJJJYLUMLA6P2TWUQ",
		"secretAccessKey" : "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"
	},
	"app" : {
		"recordProcessor" : {
			"streamName" : "words",
			"sleepSeconds" : 5,
			"checkpointRetries" : 5,
			"checkpointFreqSeconds" : 60
		}
	}
}

module.exports = settings