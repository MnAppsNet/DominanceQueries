package DominanceQueries

import ujson.read
import ujson.Value
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

object Tools {

    def log(msg:String,verbose:Boolean = false):Unit ={
        if (!verbose) return
        val id = ">>>> " //Used to identify the logging that comes from our code
        print(Console.BLUE+" "+id+Console.GREEN+" "+msg+"\n")
    }

    def getPath(fileName:String):String = {
        val currentDir = System.getProperty("user.dir")
        currentDir + "/" + fileName
    }
    
    def readSettings(settingsPath:String) = {
        log("Settings file : "+getPath(settingsPath),true)
        val json = Source.fromFile(getPath(settingsPath)).mkString
        val settings = read(json)
        val testCaseIndex = settings("activeTestCase").value.asInstanceOf[Double].toInt
        settings("testCases").arr(testCaseIndex)
    }
    def savePoints(points:Array[Point],path:String):Unit = {
        savePoints(ArrayBuffer.empty[Point]++points,path)
    }
    def savePoints(points:ArrayBuffer[Point],path:String):Unit = {
        log("Task 1 results saved in file '"+path+"'...")
        val file = new File(path)
        val bw = new BufferedWriter(new FileWriter(file))
        for (e <- points) {
            bw.write(e.getString(false)+"\n")
        }
        bw.close()
    }
    def printPoints(points:Array[Point],verbose:Boolean){
        printPoints(ArrayBuffer.empty[Point]++points,verbose)
    }
    def printPoints(points:ArrayBuffer[Point],verbose:Boolean){
        for(e <- points){
            log(e.getString(),verbose)
        }
    }

    case class Skyline(var points: ArrayBuffer[Point], var i:Int){
        def checkPoint(point:Point): Skyline = {
            //Check if point is a skyline point
            if (points.length == 0) {
                points += point //<= If no skyline points, initiate it...
                i = 0
                this
            }else {
                var loop = true
                var skylinePoint = true //Indicates whether the given point is skyline
                var j = 0
                while(loop){
                    try{
                        if (point.dominates(points(j))){
                            points.remove(j) //Point is dominated, it's not a skyline point
                            j -= 1
                        }
                        else if (points(j).dominates(point)) {
                            j = points.length
                            skylinePoint = false //The given point is dominated, don't keep it...
                        }
                    }catch{  case _: Throwable => }
                    j+=1
                    loop = j < points.length
                }
                if (skylinePoint) points += point
                i = i + 1
                this
            }
        }
        def print(verbose:Boolean):Unit = {
            log("Skyline Points :")
            printPoints(points,verbose)
        }
        def save(path:String):Unit = {
            savePoints(points,path)
        }
        def isSkyline(point:Point):Boolean = {
            points.exists(e => e.coordinates.zip(point.coordinates).forall(e => e._1 == e._2))
        }
    }

    case class PointDominations(points:RDD[(Point,Long)]){
        def getDominations(point:Point):Long = {
            //All possible dominations must have a sum of their coordinates 
            //greater or equal than those of the given point
            this.points.filter(e => e._1.sum >= point.sum)     //Get only the points with greater or equal sum
                  .filter(e => point.dominates(e._1) )         //Get the points that are dominated by the given point
                  .count( )                                 //Get the number of dominated points
        }
    }

    case class Point(coordinates: Array[Double],var sum:Double) {
            //Check if point is dominated by the given point
            def dominates(point: Point): Boolean = {
                this.coordinates.zip(point.coordinates).forall(e => e._2 >= e._1)
            }
            def getSum() {
                //Get the sum of all the coordinates
                this.sum = 0
                for(i <- 0 until coordinates.length){
                    this.sum = this.sum + coordinates(i)
                }
            }
            def getString(withSums:Boolean = true):String = {
                var res = this.coordinates.mkString(",")
                if (withSums) res+" (sum:"+sum.toString()+")"
                else res
            }
        }
}