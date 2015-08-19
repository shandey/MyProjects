package com.move.scala.renovate

import org.json4s._
import org.json4s.native.JsonMethods._
import org.joda.time._
import org.joda.time.format._
import scala.io.Source 
import scala.reflect.io.File



class RenovateTranslater (refJson:String, sourceIndicator: String, XformIndicator: String, tagSplitter: String, eventKVSplitter: String)
{
  
  //val refJson = Source.fromFile("Y:/biggie/EDWBigData/app/process/scala/renovate/resource/mdcTrailsEventLegacyMapping.json").getLines.mkString 
  val refMapJson = parse(refJson) 
  val refMapJsonAst =  refMapJson \ sourceIndicator  
  //println("refMapJsonAst: "+ refMapJsonAst)
  val refMapJsonTags = refMapJsonAst \\ classOf[JObject]
  val refMapJsonTagset = refMapJsonTags(0).keys.toSet
  
  val refXformJsonAst = refMapJson \ XformIndicator
  
  def dataReverse (Data:String): Set[(String, List[String])] = {
    val dataJson = parse(Data) 
    val dataJsonAst = dataJson
    val dataJsonTags = dataJsonAst \\ classOf[JObject]
    val dataJsonTagSet= dataJsonTags(0).keys.toSet
    
    //println("dataJsonTagSet: " + dataJsonTagSet)
    
    val TagsToTranslate = dataJsonTagSet & refMapJsonTagset 	// Set of tags that intersect, means they need translation.
    val TagsToCarryOver = dataJsonTagSet -- refMapJsonTagset 	// Values that don't need to be translated
    val TagsToProcess = TagsToTranslate ++ TagsToCarryOver		//Total set of tags to be processed.

    val dataOut = TagsToProcess.map {univTag: String => 		//Loop through the set of Total set of tags      

    			val univTagAst = dataJsonAst \ univTag			//Find the json object in Data JSON for the given tag.
      			
    			//Below construct for any Data tags that have array as value for given tag. For ex. listing_ids in SRP event.
    			val univTagValue : List[Any]  = univTagAst match { 
	                case univTagAst : JArray => (univTagAst \\ classOf[JArray])(0) 
	                case univTagAst : JString => List(univTagAst.values.toString) 
	                case univTagAst : JInt	=> List(univTagAst.values.toString) 
	                case _ => List("") 
	                }										//This will produce a string OR array of the value. Hence List. For the given tag.
    			
      			val univTagValueFixed =  univTagValue.map {_.toString} 
    			
    			val domainTagAst = refMapJsonAst \ univTag \"legacyTag"
    			val domainTag =  domainTagAst match { 
                	case JNothing => univTag 
                	case _ => domainTagAst.values.toString 
    			} 

    			val domainVal :  List[String]= univTagValueFixed.map {uv => uv match 
      			  { 
                	case uv: String => refMapJsonAst \ univTag \ "conversion" \ uv  match 
	                	{ 
	                    case JNothing => uv.toString 
	                    case _ => (refMapJsonAst \ univTag \ "conversion" \ uv).values.toString 
	                    } 
                	case _ => "" 
      			  } 
      			}     			
    			(domainTag.toString,domainVal)
    			
    }
    return dataOut    
  }
  
	protected def flattenUri(rawUriData: Set[(String, List[String])]) ={ 
	  rawUriData.flatMap { outer => outer._2.map { inner:String => (outer._1, inner )}  } 
	   
	} 
	 
	protected def uriFormat(uri : Set[(String,String)] ) :String = { 
	  val formattedUri = uri.map {t :(String,String) => t._1  + eventKVSplitter + t._2 }.mkString(tagSplitter) 
	  formattedUri 
	} 
	 
	protected def flattenReverse(data:String):Set[(String,String)] = { 
	  val reversedData = dataReverse(data) 
	  val result =flattenUri( reversedData) 
	  result 
	} 
	 
	def revertUri(data:String):String = { 
		val legactUri = flattenReverse(data) 
	  	uriFormat(legactUri) 
	} 
	
	
	def addDefaultTags(rawUriData: Set[(String, List[String])]): Set[(String, List[String])] = {	  
	  //var rawUriDataOut = rawUriData 
	  val tagDomainkey:String = "dmn"
	  val tagDomainVal:List[String] = List("www.realtor.com")  
	  
	  val tagtzkey:String = "tz"
	  val tagtzval:List[String] = List("-7".toString())
	  //rawUriDataOut = rawUriData.+((tagtzkey, tagtzval))
	  val rawUriDataOut = rawUriData.+((tagtzkey, tagtzval), (tagDomainkey, tagDomainVal))
	  rawUriDataOut
	}
	
	  	//Below procedure to process substitute order.
  	def substituteOrder(UriMap: Map[String, List[String]] ): Map[String, List[String]] = {
  	  //println ("UriMap:" + UriMap)
	  var tagFinal:String = "N"

	  val refXformJsonTag = refXformJsonAst \ "substitution"		//Find JSON object for key "substitution"
	  val refXformJsonTagAst = refXformJsonTag\\classOf[JObject]
	  val refXformJsonTagAstMap= refXformJsonTagAst(0).keys.toSet	//Make a list of tags that needs derivation by substitution.
	  //println("refXformJsonTagAstMap: " + refXformJsonTagAstMap)
	  var newUriMap = UriMap
	  
	  val refXformOut = refXformJsonTagAstMap.map {subTag :String =>	//For the list of tags that needs derivation by substitution, cycle throu them to process each.
	    			tagFinal = "N"
	    			val subTagDef = refXformJsonTag \ subTag \ "order"
	    			//println("subTagDef: " + subTagDef.children)
	    			
	    			for (tag <- subTagDef.children){					//For each ordered substitution, cycle through the most preferred to least preferred tag, as ordered by JSON. 
	    																//tagFinal flag will indicate if the preferred non-empty tag has been found.
	    																//Logic here is: Cycle through from the top of the order. If empty, delete the tag. If not empty, and the tagFinal is not "Y", this is the value we want to return. 
	    																//Add the KV pair to map using substitution tag. Delete the tags that were used for substitution. Set tagFinal = 'Y' indicating the the most preferred values have been found
	    																//Continue cycling to delete rest of the tags used for substitution.
	    			  
	    			  if (newUriMap.get(tag.values.toString()).isEmpty) {	
	    			    newUriMap -= tag.values.toString
	    			    }else{
	    			      if (tagFinal == "Y"){
	    			        newUriMap -= tag.values.toString
	    			      }else{
	    			        val list1: List[String] = newUriMap.get(tag.values.toString).get
	    			        println("Tag: " + tag.values.toString)
	    			        println("List1: " + list1(0))
	    			        println("List1 datatype: " + list1(0).getClass())
	    			        println("tag Datatype: " + tag.toString().getClass())
	    			        
	    			        if (tag.values.toString == "list_id" && list1(0) == "0"){
	    			        	newUriMap -= tag.values.toString
	    			        	println("I am here 2")
	    			        }else{
	    			        	newUriMap += ((subTag, list1))
	    			        	newUriMap -= tag.values.toString
	    			        	tagFinal = "Y"
	    			        	println("I am here 1")
	    			        }
	    			    }	    			      
	    			    }	
	    			}   			
	  }   
	  newUriMap
	}
  	
  	def customReformat(UriMap: Map[String, List[String]] ): Map[String, List[String]] = {
  	  //This method takes care of converting the values to specific formats if needed.
  	  //Convert "tm" to epoch time.
  	  var newUriMap = UriMap
  	  if (!newUriMap.get("tm").isEmpty) {
  	    val strDateTime = newUriMap.getOrElse("tm",List("-")).head
  	    try{
  	    val epochTime = ISODateTimeFormat.dateTime().parseDateTime(strDateTime).getMillis().toString
  	    newUriMap = UriMap + ("tm"-> List(epochTime))
  	    } catch{
  	      case ex: IllegalArgumentException =>{
  	        println("Value Date_Time not found")
  	      }
  	    }
  	  }
  	  newUriMap
  	}

  
  	def revertUriWithIISFormat (data:String) = { 
	  	val legactUri1 = dataReverse(data) 
	  	val legactUri = addDefaultTags(legactUri1)
	  	val legactUriMap1 = legactUri.toMap
	  	//println("legactUriMap1: " + legactUriMap1)
	  	val legactUriMap = substituteOrder(legactUriMap1)
	  	val defaultFiller = List("-") 
	  	val strDateTime = legactUriMap.getOrElse("tm",defaultFiller).head
	  	val dt:DateTime = ISODateTimeFormat.dateTime().parseDateTime(strDateTime)
	  	//val dtfmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
	  	val dtfmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)
	  	val date = dtfmt.print(dt)  
	  	//val timefmt:DateTimeFormatter = DateTimeFormat.forPattern("HH:mm:ss").withZoneUTC()
	  	val timefmt:DateTimeFormatter = DateTimeFormat.forPattern("HH:mm:ss").withZone(DateTimeZone.UTC)
	  	val time = timefmt.print(dt)
	  	val legactUriMap2 = customReformat(legactUriMap)
		val sSitename = "W3SVC84362506" 
		val sIp = "127.0.0.1" 
		val csMethod = "GET" 
		val csUriStem = "/b.ashx" 
		//println("legactUriMap: " + legactUriMap)  
		val legactUriSet = legactUriMap2.toSet
		//println("legactUriSet: " + legactUriSet)  
		val csUriQuery = uriFormat(flattenUri(legactUriSet)) 
		//println("csUriQuery: " + csUriQuery)
		val sPort = "81" 
		val csUsername = "-" 
		//val cIp = "127.0.0.1" 
		//val cIp: String = legactUriMap2.getOrElse("ip_address",List("127.0.0.1")).head
		val cIp: String = legactUriMap2.getOrElse("ClientIP",List("127.0.0.1")).head  
		val csVersion = "HTTP/1.1" 
		val csUserAgent = legactUriMap.getOrElse("UserAgent",defaultFiller).head 
		//val csUserAgent = legactUriMap2.getOrElse("user_agent",defaultFiller).head  
		val csCookie = "-" 
		val csReferer = legactUriMap2.getOrElse("referer", defaultFiller).head 
		val csHost = "rdc-b.mdctrail.com" 
		val scStatus = "200" 
		val scSubstatus = "0" 
		val scWin32Status = "0" 
		val timeTaken = legactUriMap2.getOrElse("timeTaken",List("0")).head 
		(date, time, sSitename, sIp, csMethod,  
			csUriStem, csUriQuery, sPort, csUsername,  
			cIp, csVersion, csUserAgent, csCookie,  
			csReferer, csHost, scStatus, scSubstatus,  
			scWin32Status, timeTaken)     
 } 
  
}

object RenovateTranslater {
  def refJson = Source.fromFile("mdcTrailsEventLegacyMapping.json").getLines.mkString
  //def refJson = Source.fromFile("../resource/mdcTrailsEventLegacyMapping.json").getLines.mkString
 
}
