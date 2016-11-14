/**
  * Created by SG0952655 on 10/18/2016.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.matching.Regex


object hotelhodsrrequest_parsing {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("hotelhodsrrequest_parsing"))
    val textFile = sc.textFile("/user/hive/warehouse/ehotel.db/hotelhodsrrequest/year=2016/month=01/day=*/*.gz")

    //filter by PCC
    val PCCArray = textFile.map{line =>  line.split('|')}.filter{line => line{4} == "B7ZB"}

    //GroupBy Date
    val shopperdaycount = PCCArray.map(line =>  (line{0}.split(" "){0}, 1)).reduceByKey{_+_}
    shopperdaycount.saveAsTextFile("/user/sg952655/shopperdaycount001/")
    //val shopperdaycounttxt = sc.textFile("/user/sg952655/shopperdaycount001/*")
    //val preview0 = shopperdaycounttxt.collect()

    //Duplicate record count with sessionid, transactionid, propertycode as unique key
    val filteredduplicaterecordcount = PCCArray.map(line =>  (line{1}+" "+line{2}+" "+line{6}, 1)).reduceByKey(_+_).filter{line =>  line._2 > 1}
    val Orderedfilteredduplicaterecordcount = filteredduplicaterecordcount.map(line => (line._2, line._1))
    Orderedfilteredduplicaterecordcount.saveAsTextFile("/user/sg952655/Orderedfilteredduplicaterecordcount001/")
    //val Orderedfilteredduplicaterecordcounttxt = sc.textFile("/user/sg952655/Orderedfilteredduplicaterecordcount001/*")
    //val preview1 = Orderedfilteredduplicaterecordcounttxt.collect()

    //RequestText Parsing
    val pattern = new Regex("[0-9]?[0-9][A-Za-z]{3}-[0-9]?[0-9][A-Za-z]?[A-Za-z]{2}[0-9]?[0-9]")
    val rqtparsing = PCCArray.filter(line => ((line{7}.nonEmpty) && pattern.findFirstIn(line{7}) != None)).map(line => (line{0}.split(" "){0}, line{1}, line{2}, line{6}, pattern.findFirstIn(line{7})))
    rqtparsing.saveAsTextFile("/user/sg952655/rqtparsing001/")
    //val rqtparsingtxt = sc.textFile("/user/sg952655/rqtparsing001/*")
    //val preview2 = rqtparsingtxt.collect()

    //Duplicate record count with sessionid, transactionid, propertycode, RequestText as unique key
    val rqtcount = rqtparsing.map(line => ((line._1+" "+line._2+" "+line._3+" "+line._4+" "+line._5), 1)).reduceByKey{_+_}.filter{line =>  line._2 > 1}.map(line => (line._2, line._1))
    rqtcount.saveAsTextFile("/user/sg952655/rqtcount001/")
    //val rqtcounttxt = sc.textFile("/user/sg952655/rqtcount001/*")
    //val preview3 = rqtcounttxt.collect()

    //println("shopperdaycount list of tuples: ")
    //println(preview0)

    //println("Orderedfilteredduplicaterecordcount list of tuples: ")
    //println(preview1)

    //println("rqtparsing list of tuples: ")
    //println(preview2)

    //println("rqtcounttxt list of tuples: ")
    //println(preview3)

  }

}
