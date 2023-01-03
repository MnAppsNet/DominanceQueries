package DominanceQueries

import Tools._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.nio.file.Files
import java.nio.file.Paths

object DominanceQueries {

    def main(args: Array[String]): Unit = {

        val settings = readSettings("settings.json").asInstanceOf[Option[Map[String,Any]]].get

        //Config :
        val verbose = settings.get("verbose").asInstanceOf[Option[Boolean]].get
        val inputFile = settings.get("dataFile").asInstanceOf[Option[String]].get
        val outputFolder = getPath(settings.get("outputFolder").asInstanceOf[Option[String]].get)

        Files.createDirectories(Paths.get(outputFolder));

        //Get file paths :
        val dataFile = getPath(inputFile)
        val task1File = outputFolder + "/task1.csv"
        val task2File = outputFolder + "/task2.csv"
        val task3File = outputFolder + "/task3.csv"

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