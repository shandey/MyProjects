package com.move
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

object genFunc {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(515); 
  println("Welcome to the Scala worksheet");$skip(135); 
  
   //val conf = new SparkConf().setAppName("xxx")
  //val sc = new SparkContext(conf)
   val sc = new SparkContext("local", "test");System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(34); 

    val sqc = new SQLContext(sc);System.out.println("""sqc  : org.apache.spark.sql.SQLContext = """ + $show(sqc ));$skip(91); 
    val file1 = "file:///Users/sshroff/omnidata/01-homerealtor_20150722-000000-2lines.tsv";System.out.println("""file1  : String = """ + $show(file1 ));$skip(121); 
    val df = sqc.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\t").load(file1);System.out.println("""df  : org.apache.spark.sql.DataFrame = """ + $show(df ));$skip(40); 
    
    
    


    val myRdd = df.rdd;System.out.println("""myRdd  : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = """ + $show(myRdd ));$skip(467); 
    //val clmnt = myRdd.aggregate(Set.empty[String])( {(s, m) => s union m.keySet }, { (s1, s2) => s1 union s2 })
    //println("count is "+ clmnt)
    //val newRdd = myRdd.map(row => row ++ Array((row(1).toLong * row(199).toLong).toString))

    //val newRdd = myRdd.map(row => row ++ Array((row(1).toLong * row(199).toLong).toString))
    
    val theschema = sc.textFile("file:///Users/sshroff/omnidata/homerealtor_20150722-000000-lookup_data/column_headers.tsv");System.out.println("""theschema  : org.apache.spark.rdd.RDD[String] = """ + $show(theschema ));$skip(42); 
    
    val firstHdr = theschema.first();System.out.println("""firstHdr  : String = """ + $show(firstHdr ));$skip(109); 
    val schema = StructType(firstHdr.split("\t").map(fieldName => StructField(fieldName, StringType, true)));System.out.println("""schema  : org.apache.spark.sql.types.StructType = """ + $show(schema ));$skip(61); 

    val omniDataFrame = sqc.createDataFrame(df.rdd, schema);System.out.println("""omniDataFrame  : org.apache.spark.sql.DataFrame = """ + $show(omniDataFrame ));$skip(52); 
    val persons = omniDataFrame.select("date_time");System.out.println("""persons  : org.apache.spark.sql.DataFrame = """ + $show(persons ));$skip(88); 
    val finalDf = persons.withColumn("dddd", persons("date_time")-persons("date_time"));System.out.println("""finalDf  : org.apache.spark.sql.DataFrame = """ + $show(finalDf ));$skip(54); 
    //finalDf.
    finalDf.foreach { x => println(x)}}
    
    
    //val firstHdr = theschema.first()
    //println(firstHdr)
    
  
}
