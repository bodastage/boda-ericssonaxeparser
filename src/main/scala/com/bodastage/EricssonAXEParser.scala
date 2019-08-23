package com.bodastage

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import scopt.OParser
import scala.io.Source
import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import com.bodastage.ParserStates._
import scala.collection.JavaConversions._
import scala.util.matching.Regex

case class Config(
                   in: File = new File("."),
                   out: File = null,
                   version : Boolean = false
                 )
				 
object EricssonAXEParser {
  var outputFolder: String = "";
  
  var currentState : ParserStates = EXTRACTING_PARAMETERS;
  
  //MO/Command and list of parameters 
  var moColumns = new HashMap[String, Array[String]]();
  var moPWs = new HashMap[String, PrintWriter]();
  
  //A count of the number of rows with parameter names per command session
  //This is used to determine how to collect the values
  var moParamRows = new HashMap[String, Int]();

  def main(args: Array[String]): Unit = {

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("boda-ericssonaxeparser"),
        head("boda-ericssonaxeparser", "0.0.2"),
        opt[File]('i', "in")
          .required()
          .valueName("<file>")
          .action((x, c) => c.copy(in = x))
          .validate(f =>
            if( (!Files.isRegularFile(f.toPath) && !Files.isDirectory(f.toPath))
              && !Files.isReadable(f.toPath)) failure(s"Failed to access input file/directory called ${f.getName}")
            else success
          )
          .text("input file or directory, required."),
        opt[File]('o', "out")
		  .required()
          .valueName("<file>")
          .action((x, c) => c.copy(out = x))
          .validate(f =>
            if( !Files.isDirectory(f.toPath ) && !Files.isReadable(f.toPath)) failure(s"Failed to access outputdirectory called ${f.getName}")
            else success
          )
          .text("output directory required."),
        opt[Unit]('v', "version")
          .action((_, c) => c.copy(version = true))
          .text("Show version"),
        help("help").text("prints this usage text"),
        note(sys.props("line.separator")),
        note("Parses Ericsson AXE dumps to csv."),
        note("Examples:"),
        note("java -jar boda-ericssonaxeparser.jar -i FILENAME.xml -o /path/to/folder"),
        note(sys.props("line.separator")),
        note("Copyright (c) 2019 Bodastage Solutions(https://www.bodastage.com)")

      )
    }

    var inputFile : String = ""
    var outFile : File = null;
    var showVersion : Boolean = false;
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        inputFile = config.in.getAbsolutePath
        outFile = config.out
        showVersion = config.version
      case _ =>
        // arguments are bad, error message will have been displayed
        sys.exit(1)
    }

    try{

      if(showVersion){
        println("0.0.2")
        sys.exit(0);
      }

      if(outFile != null) outputFolder = outFile.getAbsoluteFile().toString


      this.processInputFileOrDir(inputFile)

    }catch{
      case ex: Exception => {
        println("Error accessing file")
        sys.exit(1)
      }
    }

  }
  
  /**
  * Run through parameter extraction and value extraction
  */
  def processInputFileOrDir(fileName : String){
	if(currentState == EXTRACTING_PARAMETERS){
		processFileOrDirectory(fileName)
		
		currentState = EXTRACTING_VALUES;
	}


	
	//Write printer writers and  headers 
	moColumns.foreach(kv => {
		val moName : String = kv._1;
		val moParameters = kv._2;
		val fileBaseName: String  = getFileBaseName(fileName);
		
		val header : String = moParameters.map(p => toCSVFormat(p)).mkString(",")
		
		val csvFile: String = outputFolder + File.separator + s"${moName}.csv";
		
		val pw = new PrintWriter(new File(csvFile));
		pw.println("FILENAME,DATETIME,NODENAME,MML," + header);

		moPWs.put(moName, pw)
		
	})
	
	processFileOrDirectory(fileName)
	
	//Close print writers 
	moPWs.foreach( kv => kv._2.close());
	
  }

  /**
    * Get file base name
    *
    * @param fileName
    * @return
    */
  def getFileBaseName(fileName: String): String ={
    try{
      return new File(fileName).getName
    }catch{
      case ex: Exception => {
        return fileName
      }
    }
  }


  def processFileOrDirectory(inputPath: String): Unit ={


    val file : Path = Paths.get(inputPath)
    val isRegularExecutableFile : Boolean = Files.isRegularFile(file) & Files.isReadable(file)
    val isReadableDirectory = Files.isDirectory(file) & Files.isReadable(file)

    if (isRegularExecutableFile) {
      this.parseFile(inputPath)
    }

    if (isReadableDirectory) {
      val directory = new File(inputPath)

      val fList = directory.listFiles
      for(f:File <- fList){
        try {
          if( Files.isRegularFile(f.toPath)){
            this.parseFile(f.getAbsolutePath)
          }else{
            println(s"${f.getAbsolutePath} is not a regular file. Skipping it.")
          }

        }catch {
          case ex: Exception => {
            println(s"Error processing ${f.getAbsolutePath}")
			println(ex.toString)
          }
        }
      }
    }
  }

  def getDateTime() : String = {
    //Get current date and time
	val dateFormat : DateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	val date : Date = new Date();
	val dateTime : String = dateFormat.format(date);
	return dateTime;
  }
  
    
  /**
    * Parse a file
    * @param filename
    */
  def parseFile(fileName: String) : Unit = {
    val fileBaseName: String  = getFileBaseName(fileName);
	var inCmdHeader : Boolean = false;
	val currentDateTime : String = getDateTime;
	
	//Track/cache previous 3 lines
	var prevLine : String = "";
	var prevLine2 : String = "";
	var prevLine3 : String = "";
	
	//Parameter section start line
	var paramSecStartLine : String = "";
	var inParamSection : Boolean  = false;
	
	//parameters 
    var parameters : Array[String] = Array[String]()
	var paramValues : Array[String] = Array[String]()
	
	//Line with the most recent parameters 
	var currentParamLine : String = "";
	var currentLineParams  = Array[String]();
	//The start and end indices marking the values of the parameters in 
	//currentParamLine
	var currParamLineValIdxes  = Array[Array[Int]]();
	var currentSectionParamLines = new HashMap[String, Int]();
	var cmdSectionParamLineCount : Int = 0;
	
	var responseReceptionInterrupted: Boolean = false;

	//Command section line counter. 
	var sLineCount : Integer = 0;
	
	var sectionCommand : String = "";
	var sectionMML : String = "";
	
	var nodename = "";
	
    var lineCount : Integer = 0;

    var header = "";
    var headerArray : Array[String] =  Array[String]()
	
    for (line <- Source.fromFile(fileName).getLines) {
      lineCount += 1;
	  //println(s"${line}")
	  
      breakable {
		
		//Skip empty lines
		if(line.trim.length == 0) break;
		
	  
		//Get nodename
	    if("(?i)(.*eaw\\s+.*;)".r.findAllIn(line).length == 1){
			val pattern = ".*eaw\\s+(.*);".r
			val pattern(node) = line
			nodename = node.trim
			//println(nodename);
			break;
		}
		
		//Skip lines with NONE 
		if(line.trim == "NONE" && prevLine.trim.length == 0) break;
	  
		 //skip lines following "RESPONSE RECEPTION INTERRUPTED"
		 if(responseReceptionInterrupted == true  && line.length > 0 ) break;
		 if(responseReceptionInterrupted == true  && line.length == 0 ) { 
			responseReceptionInterrupted = false;
			break;
		 }
	  
		 //skip line with quit 
	     if(line.replace("\\s+","").toLowerCase == "<quit;") break;
		 if(line.trim == "RESPONSE RECEPTION INTERRUPTED" ){
			responseReceptionInterrupted = true;
			break;
		 }
		 
		//Reset 
		if(line.trim == "END"){
			
			if(currentState == EXTRACTING_PARAMETERS && cmdSectionParamLineCount > 0){
				moParamRows.replace(sectionCommand, cmdSectionParamLineCount)
			}
			
			//Printout the last values
			if( paramValues.length > 0 && sectionCommand.length > 0 && currentState == EXTRACTING_VALUES) {			
				if(moPWs.containsKey(sectionCommand) && moParamRows.get(sectionCommand) > 1){
					moPWs.get(sectionCommand).println(
						s"${toCSVFormat(fileBaseName)}," + 
						s"${currentDateTime}," + 
						s"${nodename}," + 
						s"${toCSVFormat(sectionMML)}," + 
						paramValues.mkString(","));
				}
			}
			
			paramValues = Array[String]();
			
			sLineCount = 0;
			inCmdHeader = false;
			sectionCommand = "";
			sectionMML = "";
			paramSecStartLine = "";
			currentParamLine = "";
			inParamSection = false;
			
			break;
		}
		  
		if(sectionCommand.length > 0){
			sLineCount += 1;
		}
	  

		
		//Reset section variables
		if(line.slice(0,1) == "<"){
			//Reset the section line counter to 1
			sLineCount = 1;
			inCmdHeader = true;
			inParamSection = false;
			
			val pattern = "([^<:;]+)".r
			sectionCommand = (pattern findFirstIn line).get.trim.toUpperCase
			sectionMML = line.trim.replaceAll("^<|;$", "")
			
			if(currentState == EXTRACTING_PARAMETERS && cmdSectionParamLineCount > 0){
				moParamRows.replace(sectionCommand, cmdSectionParamLineCount)
			}
			
			//Reset 
			cmdSectionParamLineCount = 0
			
			if(currentState == EXTRACTING_PARAMETERS){
				if(!moColumns.containsKey(sectionCommand)){
					moColumns.put(sectionCommand, Array[String]());
					moParamRows.put(sectionCommand, 0)
				}
			}
						
			//Set parameter values array to empty string at beginning of command section
			if(currentState == EXTRACTING_VALUES){
				paramValues = moColumns.get(sectionCommand).map( v => "")
			}
			
			break;
		}
		
		//If we encouter the paramSecStartLine line 
		//End of parameter section 
		if(line.trim == paramSecStartLine.trim && line.trim.length > 0){			
			//Print values to file 
			
			if(currentState == EXTRACTING_VALUES){
				moPWs.get(sectionCommand).println(
					s"${toCSVFormat(fileBaseName)}," + 
					s"${currentDateTime}," + 
					s"${nodename}," + 
					s"${toCSVFormat(sectionMML)}," + 
					paramValues.mkString(","));

				//Reset parameter values
				paramValues = moColumns.get(sectionCommand).map( v => "")
			}
			
			//Cliear repeatition tracker
			currentSectionParamLines.clear()
			
		}
		
		//Get the parameter values 
		if(currentState == EXTRACTING_VALUES && inParamSection == true && 
		   line.trim.length > 0 && prevLine.trim.length > 0){
			//println("currentLine: " + line)
			//println(s"currentParamLine: ${currentParamLine}")
			//println("currentLineParams:" + currentLineParams.mkString(","))
			//println("value indices:" + currParamLineValIdxes.map( m => m.mkString("-")).mkString(",") )
			//println(s"paramValues count: ${paramValues.length}")
			//println(s"moParamRows.get(sectionCommand): ${moParamRows.get(sectionCommand)}")
						
			for(i <- 0 to currentLineParams.length-1){
			
				breakable{
					val paramName  = currentLineParams(i)
					val indexInParamArray = moColumns.get(sectionCommand).indexOf(paramName)
					
					//Get value start and end indices 
					val valIndices = currParamLineValIdxes(i)
					
					val startIndex =valIndices(0)
					val endIndex = if(i < currentLineParams.length-1 ) valIndices(1) else line.length
					val value = line.slice(startIndex, endIndex).trim;
					
					paramValues(indexInParamArray) = value
				}
			}
			
			//Print values for commands with 1 parameter line 
			if(moParamRows.get(sectionCommand) == 1 ){
				moPWs.get(sectionCommand).println(
					s"${toCSVFormat(fileBaseName)}," + 
					s"${currentDateTime}," + 
					s"${nodename}," + 
					s"${toCSVFormat(sectionMML)}," + 
					paramValues.mkString(","));

				//Reset parameter values
				paramValues = moColumns.get(sectionCommand).map( v => "")
			}
		}
		
		//Mark start of parameter section
		//Current line is not empty (line.length > 0)
		//Previous line is empty (prevLine.length == 0)
		if(inCmdHeader == true && line.length > 0 && prevLine.length == 0){		
			paramSecStartLine = line;
			inParamSection = true;
			inCmdHeader = false;
			
			if(currentState == EXTRACTING_VALUES){
				//Set the values to empty strings 
				paramValues = moColumns.get(sectionCommand).map( v => "")			
			}

		}
		
		
		//Occurrences
		if(currentState == EXTRACTING_VALUES && inParamSection == true && 
		   line.length > 0 && prevLine.length == 0){
			if(currentSectionParamLines.containsKey(line)){
				//Printout values whenever a repeatition is found
				if(currentSectionParamLines.get(line) > 0 ){
					moPWs.get(sectionCommand).println(
						s"${toCSVFormat(fileBaseName)}," + 
						s"${currentDateTime}," + 
						s"${nodename}," + 
						s"${toCSVFormat(sectionMML)}," + 
						paramValues.mkString(","));
						
					currentSectionParamLines.clear();	
						
				}
			}
			
		}

		//Collect parameters
		if(currentState == EXTRACTING_PARAMETERS && inParamSection == true 
			&& line.length > 0 && prevLine.length == 0){
			currentParamLine = line;
			var paramsInLine = line.split("\\s{2}").map(v => v.trim).filter(v => v.length > 0)
			var parameters = moColumns.get(sectionCommand)
			for(i <- 0 to paramsInLine.length - 1){
				if( !(parameters contains paramsInLine(i))){
					parameters = parameters :+ paramsInLine(i)
					paramValues = paramValues :+ "";
				}
			}
			
			//
			cmdSectionParamLineCount += 1
			
			//Add parameter to moColumns 
			moColumns.replace(sectionCommand, parameters);
			
		}
		
		//Collect parameter VALUES
		if(currentState == EXTRACTING_VALUES && inParamSection == true && line.length > 0 && prevLine.length == 0){
			currentParamLine = line;

			//Current parameter line parameters 
			currentLineParams = currentParamLine.split("\\s{2}").map(v => v.trim).filter(v => v.length > 0)
			currParamLineValIdxes = Array[Array[Int]]()
			for(i <- 0 to currentLineParams.length-1){
				val startIdx = if(i == 0) 0 else line.indexOf(currentLineParams(i))
				val endIdx = if(i < currentLineParams.length-1) line.indexOf(currentLineParams(i+1)) else line.length
				currParamLineValIdxes = currParamLineValIdxes :+  Array(startIdx, endIdx)
			}
			
			//Add the line and the number of occurences in the section
			if(currentSectionParamLines.containsKey(line)){
				val occurences = currentSectionParamLines.get(line)
				currentSectionParamLines.put(line, occurences + 1)
			}else{
				currentSectionParamLines.put(line, 1)
			}
			
		}

						
	}
	prevLine3 = prevLine2;
	prevLine2 = prevLine;
	prevLine = line;
    }

  }

  /*
  * Type 1: RXMOP+RXMFP
  *
  * @param string line
  * @param string line2
  * @param string line3
  * @param Integer lineCount
  * @param Integer sLineCount
  */
  def parseType1(line : String, line2 : String, line3 : String, lineCount : Integer, sLineCount : Integer){
	
  }
  
  def toCSVFormat(s: String): String = {
    var csvValue: String = s

    if(s.matches(".*,.*")){
      csvValue = "\"" + s + "\""
    }

    if(s.matches(""".*".*""")){
      csvValue = "\"" + s.replace("\"", "\"\"") + "\""
    }

    return csvValue
  }

}
