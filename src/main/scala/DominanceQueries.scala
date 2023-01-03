package DominanceQueries

import Tools._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object DominanceQueries {

    def main(args: Array[String]): Unit = {

        //Config :
        val verbose = true
        val inputFile = "corelated.s1000.e0.5.csv"
        val outputFileTask1 = "task1.csv"
        val outputFileTask2 = "task2.csv"
        val outputFileTask3 = "task3.csv"


        //Get file paths :
        val dataFile = getPath(inputFile)
        val task1File = getPath(outputFileTask1)
        val task2File = getPath(outputFileTask2)
        val task3File = getPath(outputFileTask3)

        // Create spark configuration
        val sparkConfig = new SparkConf()
        .setMaster("local[2]")
        .setAppName("DominanceQueries")

        // create spark context
        val sc = new SparkContext(sparkConfig)
        
        //Read CSV into an RDD
        //>> Line format : X.XXXX,Y.YYYY,Z.ZZZZ,...
        //>> RDD[String]
        log("Reading file '" + dataFile + "'...",verbose)
        val rawData = sc.textFile(dataFile)

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
        val res = sortedData.map(e => ( skyline.checkPoint(e)))     //Check each point in the RDD if it is in skyline
                            .sortBy(_.i,ascending = false).first()  //Get the last entry of the RDD which should contain all the skyline points
        res.print(verbose)
        res.save(task1File)

        sc.stop()
    }
}