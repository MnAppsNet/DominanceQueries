package DominanceQueries

import Tools._
import ujson.Value
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object DominanceQueries {

    def main(args: Array[String]): Unit = {

        var settingsFilePath = "settings.json"
        if (args.length > 0){
            settingsFilePath = args(0)
        }
        var settingIndex = -1
        if (args.length > 1){
            settingIndex = args(1).toInt
        }

        val settings = readSettings(settingsFilePath,settingIndex)

        //Config :
        val topKpoints = settings("topKpoints").value.asInstanceOf[Double].toInt
        val cores = settings("cores").value.asInstanceOf[Double].toInt
        val inputFile = settings("dataFile").value.asInstanceOf[String]
        val executeTask2 = settings("executeTask2").value.asInstanceOf[Boolean]
        val executeTask3 = settings("executeTask3").value.asInstanceOf[Boolean]
        //No option to execute task 1 because its result is needed for the other tasks

        //Get file paths :
        val dataFile = getPath(inputFile)
        val task1File = getPath(settings("task1ResultsOutput").value.asInstanceOf[String])
        val task2File = getPath(settings("task2ResultsOutput").value.asInstanceOf[String])
        val task3File = getPath(settings("task3ResultsOutput").value.asInstanceOf[String])


        // Create spark configuration
        val sparkConfig = new SparkConf()
        .setMaster("local["+cores.toString+"]")
        .setAppName("DominanceQueries")

        // create spark context
        val sc = new SparkContext(sparkConfig)
        sc.setLogLevel("OFF")

        print("\u001b[2J")

        startTimer()

        //Read CSV into an RDD
        //>> Line format : X.XXXX,Y.YYYY,Z.ZZZZ,...
        //>> RDD[String]
        log("File set for read : " + dataFile)
        val rawData = sc.textFile(dataFile,5)

        //Split the RDD raw string lines into instances of point class
        //>> Line format : Point(X.XXXX,Y.YYYY,Z.ZZZZ,...)
        //>> RDD[Point]
        val data = rawData.map{rawLine => 
            val point = Point(rawLine.split(",").map(_.toDouble),0,0)
            point.getSum()
            point
        }
        //Sort the points based on the sum of all their dimensions
        //>> Line format : Point(X.XXXX,Y.YYYY,Z.ZZZZ,...) , Index
        //>> RDD[Long,Point]
        log("Gathering and sorting the data...")
        val sortedData = data.sortBy(_.sum, ascending = true)                   //Sort by sum
                             .zipWithIndex                                      //Add an index that follows the ascending sorting of the sums
                             .map(x => Point(x._1.coordinates,x._1.sum,x._2))   //Map to new points with the corrent index
                             .cache()
        logTime()
        //Task 1 - Get skyline points /!\ Always executed no matter what because it is needed for the rest of the tasks
        log("Task 1:")
        //Get the skyline points of each partition and then calculate the final skyline set
        //from the partition skylines
        val skylinePoints = getSkylinePoints(sortedData).to(ArrayBuffer)
        savePoints(skylinePoints,task1File)
        logTime()

        if (executeTask2){
            log("Task 2:")
            val topDominators = getTopDominators(sc,topKpoints,sortedData,skylinePoints)
            savePoints(topDominators,task2File)
            logTime()
        }

        if (executeTask3){
            log("Task 3:")
            getDominations(sortedData,skylinePoints)                     //Count skyline dominations
            val topKSkylinePoints = skylinePoints.toArray
                                                 .sortBy(_.dominations)  //Sort by the number of dominations
                                                 .reverse                //Reverse to get in descending order
                                                 .take(topKpoints)       //Get top k points based on dominations
                                                 .to(ArrayBuffer)
            savePoints(topKSkylinePoints,task3File)
            logTime()
        }

        sc.stop()
    }
}