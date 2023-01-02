package DominanceQueries

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DominanceQueries {

    val log = ">>>> " //Used to identify the logging that comes from our code

    case class Point(coordinates: Array[Double]) {
        //Check if point is dominated by the given point
        def dominatedBy(point: Point): Boolean = {
            this.coordinates.zip(point.coordinates).forall { case (x, y) => x <= y }
        }
    }

    def main(args: Array[String]): Unit = {

        //Config :
        val dim = 2
        val fileName = "corelated.s1000.e0.5.csv"

        //Get data file path :
        val currentDir = System.getProperty("user.dir")
        val file = currentDir + "/" + fileName
        println(log+"Data File : " + file)

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
        val data = rawData.map(rawLine => 
            Point(rawLine.split(",").map(_.toDouble))
        )

        //Get the number of dominators for each point
        

        sc.stop()
    }
}