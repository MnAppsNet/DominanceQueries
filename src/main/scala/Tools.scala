package DominanceQueries

import ujson.read
import ujson.Value
import scala.io.Source
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object Tools {

  var startedTime:Long = -1

  def startTimer(){
    startedTime = System.nanoTime()
  }
  def getSecondsPassed():Double = {
    if (startedTime != -1)
      ( (System.nanoTime - startedTime) / 1e9d )
    else
      0.0
  }
  def logTime(){
    log("Execution time : " + getSecondsPassed() + " sec")
    log("---")
    log("")
    startTimer()
  }
  def log(msg: String): Unit = {
    var id = ">>>> " // Used to identify the logging that comes from our code
    if (startedTime != -1) id = id + "(%.4fsec)".format(getSecondsPassed())
    print(Console.BLUE + " " + id + Console.GREEN + " " + msg + "\n")
  }

  def getPath(fileName: String): String = {
    val currentDir = System.getProperty("user.dir")
    currentDir + "/" + fileName
  }

  def readSettings(settingsPath: String, settingIndex: Int = -1) = {
    log("Reading settings file : " + getPath(settingsPath))
    val json = Source.fromFile(getPath(settingsPath)).mkString
    val settings = read(json)
    var testCaseIndex = 0
    if (settingIndex > -1)
      testCaseIndex = settingIndex
    else
      testCaseIndex =
        settings("activeTestCase").value.asInstanceOf[Double].toInt
    settings("testCases").arr(testCaseIndex)
  }

  def savePoints(points: ArrayBuffer[Point], path: String): Unit = {
    log("Saving results in : " + path)
    Files.createDirectories(Paths.get(path).getParent());
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    for (e <- points) {
      bw.write(e.getString(false) + "\n")
    }
    bw.close()
  }

  def getSkylinePoints( sortedData:RDD[Point]): Iterator[Point] = {
    //Collect all the skyline points of each partition and find the
    //final set of skyline points among them
    log("Calculating and collecting skyline points...")
    getSkylinePoints(
      sortedData.mapPartitions(getSkylinePoints)
                .collect().toIterator)
  }

  def getSkylinePoints( points: Iterator[Point] ): Iterator[Point] = {
    // Check if point is a skyline point
    val skyline = new ArrayBuffer[Point]()
    val pointsArray = points.toArray

    if (pointsArray.length < 1)
      return skyline.toIterator // If no data provided, return the empty array

    skyline += pointsArray( 0 ) // First point should be a skyline as it has the smallest sum
    for (i <- 1 until pointsArray.length) { // Loop on each given point
      var loop = true // Flag to break the loop
      var skylinePoint = true // Indicates whether the current point is skyline
      var j = 0 // Loop index
      while (loop) { // Loop on each skyline gathered so far
        if (pointsArray(i).dominates(skyline(j))) {
          skyline.remove(
            j
          ) // This skyline point is dominated, it's not a skyline
          j -= 1
        } else if (skyline(j).dominates(pointsArray(i))) {
          j = skyline.length // Set the index as such the loop is broken
          skylinePoint =
            false // The current point is dominated, don't keep it...
        }
        j += 1
        loop =
          j < skyline.length // Loop through all the skyline points collected
      }
      if (skylinePoint) skyline += pointsArray(i)
    }
    skyline.toIterator
  }

  def getTopDominators(sc:SparkContext, numberOfTopDominators:Int,sortedData:RDD[Point], skylinePoints: ArrayBuffer[Point] = null ):ArrayBuffer[Point] = 
    {
    log("Calculating the top " + numberOfTopDominators + " points in terms of dominations...")
    var tmpSkyline = new ArrayBuffer[Point]()
    skylinePoints.copyToBuffer(tmpSkyline)
    if (tmpSkyline == null) //If no skyline provided, calculate it 
      tmpSkyline = getSkylinePoints(sortedData).to(ArrayBuffer)
    
    //Calculate the dominations of the skyline data
    getDominations(sortedData,tmpSkyline)

    var topDominators = new ArrayBuffer[Point]()
    var loop = true
    var topPoints = tmpSkyline
    var allPoints = sortedData
    
    //Look for the top dominator on the skyline and close to it
    //Initially, we set the skyline as the canditate top points.
    //We then get the canditate point with most dominations and we look for the 
    //its skyline and its dominations. Then these local skyline is added to the
    //canditate top points, sort them and the procedure is repeted until we get
    //the top k points
    while(loop){
      topPoints = topPoints.sortBy(_.dominations).reverse
      //Keep the first element of the top points with most dominations
      val topPoint = topPoints.remove(0)
      topDominators += topPoint
      //Remove it from the total points RDD
      allPoints = allPoints.filter(x => x.index != topPoint.index)
      
      //Check for new canditate top points:
        
      //Get all the points dominated by the topPoint
      val topPointsBC = sc.broadcast(topPoints) //Broadcast the existing top points to exclude from the check for new canditate top poitns
      val dominatedArea = allPoints.filter(p => topPoint.dominates(p))
                                   .filter(p => topPointsBC.value.exists(x=>x.index==p.index) == false)

      //Get the skyline of the dominated area
      val dominatedAreaSkyline = getSkylinePoints(dominatedArea).to(ArrayBuffer)
      //Calculate dominance score of the dominated area skyline
      getDominations(allPoints,dominatedAreaSkyline)
      
      //Add all the dominated area skyline points to the top points
      topPoints ++= dominatedAreaSkyline

      loop = (topDominators.length < numberOfTopDominators) || topPoints.length == 0
    }
    topDominators
  }

  def getDominations(data:RDD[Point], points: ArrayBuffer[Point] ) {
    for(p <- points){
      //Cound the number of dominations for each of the given points
      p.dominations = data.filter(x => p.dominates(x)).count()
    }
  }

  case class Point(coordinates: Array[Double], var sum: Double, var index: Long, var dominations: Long = 0) {
    // Check if point is dominated by the given point
    def dominates(point: Point): Boolean = {
      this.coordinates.zip(point.coordinates).forall(e => e._2 >= e._1)
    }
    def getSum() {
      // Get the sum of all the coordinates
      this.sum = 0
      for (i <- 0 until coordinates.length) {
        this.sum = this.sum + coordinates(i)
      }
    }
    def getString(withSums: Boolean = true): String = {
      var res = this.coordinates.mkString(",")
      if (withSums) res + " (sum:" + sum.toString() + ")"
      else res
    }
  }
}
