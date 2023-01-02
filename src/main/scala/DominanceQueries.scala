package DominanceQueries

import Tools._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object DominanceQueries {

    def main(args: Array[String]): Unit = {

        //Config :
        val dim = 2
        val fileName = "corelated.s1000.e0.5.csv"

        //Get data file path :
        val currentDir = System.getProperty("user.dir")
        val file = currentDir + "/" + fileName
        log("Data File : " + file)

        // Create spark configuration
        val sparkConfig = new SparkConf()
        .setMaster("local[2]")
        .setAppName("DominanceQueries")

        // create spark context
        val sc = new SparkContext(sparkConfig)
        
        //Read CSV into an RDD 
        //>> Line format : X.XXXX,Y.YYYY,Z.ZZZZ,...
        //>> RDD[String]
        val rawData = sc.textFile(file)

        //Split the RDD raw string lines into instances of point class
        //>> Line format : Point(X.XXXX,Y.YYYY,Z.ZZZZ,...  , domination_flag{true/false})
        //>> RDD[Point]
        val i = 1
        val data = rawData.map{rawLine => 
            val point = Point(rawLine.split(",").map(_.toDouble),0)
            point.getSum()
            point
        }

        //Task 1 :
        //Sort the points based on the sum of all their dimensions
        val sortedData = data.sortBy(_.sum, ascending = true)

        //Take a quick look at the state of the data
        //log("First 5 points sorted by the sum of their coordinates :\n----------")
        //sortedData.take(5).foreach(e => log(e.getString()))

        //Get skyline points
        val skyline = new Skyline(new ArrayBuffer[Point](),0)
        val res = sortedData.map(e => ( skyline.checkPoint(e)))
        //Show Skyline points
        res.sortBy(_.i,ascending = false).first().print()

        sc.stop()
    }
}