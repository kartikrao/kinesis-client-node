Create a Javascript file named kinesis.app.words.settings.js under coffee/sample/

settings = {
	"aws" : {
		"region"  : "Your Region",
		"accessKeyId" : "Your Access Key",
		"secretAccessKey" : "Your Secret Access Key"
	},
	"app" : {
		"recordProcessor" : {
			"streamName" : "Name of Stream",
			"sleepSeconds" : "Sleep Interval (integer)",
			"checkpointRetries" : "Number of checkpoint retries (integer)",
			"checkpointFreqSeconds" : "Checkpoint frequency (integer)"
		}
	}
}