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
        .setMaster("local[4]")
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
        //Sort the points based on the sum of all their dimensions
        val sortedData = data.sortBy(_.sum, ascending = true)


        //Task 1 - Get skyline points
        val skyline = new Skyline(new ArrayBuffer[Point](),0)
        val skylineBC = sc.broadcast(skyline) //Broadcast the instance of Skyline that will handle the calculations
        sortedData.map(e => (skylineBC.value.checkPoint(e))) //Check each point in the RDD if it is in skyline
                  .count()  //An action is needed in order to execute the calculations in the broadcasted instance, that's why we do a count
        skylineBC.value.print(verbose)
        skylineBC.value.save(task1File)
        skylineBC.destroy()

        sc.stop()
    }
}