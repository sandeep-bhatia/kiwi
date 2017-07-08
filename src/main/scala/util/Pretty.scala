package kiwi

object Pretty {
	var prettyPrintingEnabled : Boolean = false
	var testLoad : Boolean = false

	def enableDebug() {
		prettyPrintingEnabled = true
	}

	def setTestLoad() {
		testLoad = true
	}

	def isDebugEnabled() : Boolean = {
		prettyPrintingEnabled 
	}

	def isTestLoad() : Boolean = {
		testLoad 
	}

	def outputInfo(str: String) {
		println(String.format("Info: %s", str))
	}

	def outputString(str: String) {
		if (prettyPrintingEnabled == true) {
			println(String.format("Info: %s", str))
			Console.RESET
		}	
	}

	def outputMap(map: scala.collection.Map[String, String]) {
		if (prettyPrintingEnabled == true) {
			map.foreach(record => {
				Pretty.outputString(String.format("%s , %s", record._1, record._2))
				Console.RESET
			})
		}
	}

	def outputError(str: String) {
		System.err.println("ERROR:", str)
		Console.RESET
	}

	def removeQuotes(input: String): String = {
 		var output = "";
 		
 		if (input.charAt(0) == '"') {
        	output = input.substring(1, input.length)
    	} else {
    		output = input
    	}

    	if(output.charAt(output.length - 1) == '"') {
    		output = output.substring(0, output.length - 1)
    	}
    	output
 	}
}
