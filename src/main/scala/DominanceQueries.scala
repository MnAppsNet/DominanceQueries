package DominanceQueries

import Tools._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

object DominanceQueries {

    def main(args: Array[String]): Unit = {

        val settings = readSettings("settings.json").asInstanceOf[Option[Map[String,Any]]].get

        //Config :
        val verbose = settings.get("verbose").asInstanceOf[Option[Boolean]].get
        val topKpoints = settings.get("topKpoints").asInstanceOf[Option[Double]].get.toInt
        val cores = settings.get("cores").asInstanceOf[Option[Double]].get.toInt
        val inputFile = settings.get("dataFile").asInstanceOf[Option[String]].get
        val executeTask2 = settings.get("executeTask2").asInstanceOf[Option[Boolean]].get
        val executeTask3 = settings.get("executeTask3").asInstanceOf[Option[Boolean]].get

        //Get file paths :
        val dataFile = getPath(inputFile)
        val task1File = getPath(settings.get("task1ResultsOutput").asInstanceOf[Option[String]].get)
        val task2File = getPath(settings.get("task2ResultsOutput").asInstanceOf[Option[String]].get)
        val task3File = getPath(settings.get("task3ResultsOutput").asInstanceOf[Option[String]].get)

        // Create spark configuration
        val sparkConfig = new SparkConf()
        .setMaster("local["+cores.toString+"]")
        .setAppName("DominanceQueries")

        // create spark context
        val sc = new SparkContext(sparkConfig)
        sc.setLogLevel("WARN")

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
        val skyline = new Skyline(new ArrayBuffer[Point](),0)
        val skylineBC = sc.broadcast(skyline) //Broadcast the instance of Skyline that will handle the calculations
        sortedData.map(e => (skylineBC.value.checkPoint(e._1))) //Check each point in the RDD if it is in skyline
                  .count()  //An action is needed in order to execute the calculations in the broadcasted instance, that's why we do a count
        skylineBC.value.print(verbose)
        skylineBC.value.save(task1File)
        log("Task 1 execution time : " + (System.nanoTime - startedAt) / 1e9d + " sec",true)
        startedAt = System.nanoTime

        //Get the dominations of each point
        val pointDominations = new PointDominations(sortedData)
        val pointDominationsBC = sc.broadcast(pointDominations) //Bradcast the class that is used to calculate the dominations

        //Task 2 - Get the top k points with most dominations
        if (executeTask2) {
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
            log("Task 2 execution time : " + (System.nanoTime - startedAt) / 1e9d + " sec",true)
            startedAt = System.nanoTime
        }

        //Task 3 - Get the top k skyline points with most dominations
        if (executeTask3) {
            val topSkylinePoints = sortedData
                                        .filter(e=>skylineBC.value.isSkyline(e._1)) //Keep only skyline points
                                        .map( e => (e._1,pointDominationsBC.value.getDominations(e._1))) //For each skyline point calculate the domination
                                        .sortBy(_._2,ascending=false) //Sort from most to less dominations
                                        .take(topKpoints) //Take the first 'topKpoints' that will have the most dominations
                                        .map(_._1) //Keep only the points and discard the dominations number
            log("Get the top k skyline points with most dominations",verbose)
            printPoints(topSkylinePoints,verbose)
            savePoints(topSkylinePoints,task3File)
            log("Task 3 execution time : " + (System.nanoTime - startedAt) / 1e9d + " sec",true)
        }

        //Task 3 - Get the top k points with most dominations from the skyline points
        skylineBC.destroy()
        pointDominationsBC.destroy()

        sc.stop()
    }
}