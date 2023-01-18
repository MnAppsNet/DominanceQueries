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
        val verbose = settings("verbose").value.asInstanceOf[Boolean]
        val topKpoints = settings("topKpoints").value.asInstanceOf[Double].toInt
        val cores = settings("cores").value.asInstanceOf[Double].toInt
        val inputFile = settings("dataFile").value.asInstanceOf[String]
        val executeTask2 = settings("executeTask2").value.asInstanceOf[Boolean]
        val executeTask3 = settings("executeTask3").value.asInstanceOf[Boolean]

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

        //Get starting time
        var startedAt = System.nanoTime

        //Read CSV into an RDD
        //>> Line format : X.XXXX,Y.YYYY,Z.ZZZZ,...
        //>> RDD[String]
        log("Reading file '" + dataFile + "'...",verbose)
        val rawData = sc.textFile(dataFile)

        //Split the RDD raw string lines into instances of point class
        //>> Line format : Point(X.XXXX,Y.YYYY,Z.ZZZZ,...)
        //>> RDD[Point]
        val i = 1
        val data = rawData.map{rawLine => 
            val point = Point(rawLine.split(",").map(_.toDouble),0)
            point.getSum()
            point
        }
        //Sort the points based on the sum of all their dimensions
        //>> Line format : Point(X.XXXX,Y.YYYY,Z.ZZZZ,...) , Index
        //>> RDD[Point,Long]
        val sortedData = data.sortBy(_.sum, ascending = true).zipWithIndex.cache()

        //Task 1 - Get skyline points /!\ Always executed no matter what because it is needed for the rest of the tasks
        log("Task 1:",true)
        val skyline = new Skyline(new ArrayBuffer[Point](),0)
        val skylineBC = sc.broadcast(skyline) //Broadcast the instance of Skyline that will handle the calculations
        sortedData.map(e => (skylineBC.value.checkPoint(e._1))) //Check each point in the RDD if it is in skyline
                  .count()  //An action is needed in order to execute the calculations in the broadcasted instance, that's why we do a count
        skylineBC.value.print(verbose)
        skylineBC.value.save(task1File)
        log("Execution time : " + (System.nanoTime - startedAt) / 1e9d + " sec",true)
        log("---",true)
        startedAt = System.nanoTime

        //Get the dominations of each point
        val pointDominations = new PointDominations(sortedData)
        val pointDominationsBC = sc.broadcast(pointDominations) //Bradcast the class that is used to calculate the dominations

        //Task 2 - Get the top k points with most dominations
        if (executeTask2) {
            log("Task 2:",true)
            //The top points in dominations will be for sure points in the skyline or close to the skyline
            //So we are creating a lookup window to check the dominations only for the first points with the smallest sums
            //up to the number of skyline points plus the number of topk points we are looking for.
            val lookupWindow = skylineBC.value.points.length + 2*topKpoints
            val topPoints = sortedData
                                    .filter(_._2 <= lookupWindow) //Keep only the first lookupWindow point with the least sums
                                    .map( e => (e._1,pointDominationsBC.value.getDominations(e._1))) //For point calculate the domination
                                    .sortBy(_._2,ascending=false) //Sort from most to less dominations
                                    .take(topKpoints) //Take the first 'topKpoints' that will have the most dominations
                                    .map(_._1) //Keep only the points and discard the dominations number
            log("Get the top k points with most dominations",verbose)
            printPoints(topPoints,verbose)
            savePoints(topPoints,task2File)
            log("Execution time : " + (System.nanoTime - startedAt) / 1e9d + " sec",true)
            log("---",true)
            startedAt = System.nanoTime
        }

        //Task 3 - Get the top k skyline points with most dominations
        if (executeTask3) {
            log("Task 3:",true)
            val topSkylinePoints = sortedData
                                        .filter(e=>skylineBC.value.isSkyline(e._1)) //Keep only skyline points
                                        .map( e => (e._1,pointDominationsBC.value.getDominations(e._1))) //For each skyline point calculate the domination
                                        .sortBy(_._2,ascending=false) //Sort from most to less dominations
                                        .take(topKpoints) //Take the first 'topKpoints' that will have the most dominations
                                        .map(_._1) //Keep only the points and discard the dominations number
            log("Get the top k skyline points with most dominations",verbose)
            printPoints(topSkylinePoints,verbose)
            savePoints(topSkylinePoints,task3File)
            log("Execution time : " + (System.nanoTime - startedAt) / 1e9d + " sec",true)
        }

        //Task 3 - Get the top k points with most dominations from the skyline points
        skylineBC.destroy()
        pointDominationsBC.destroy()

        sc.stop()
    }
}