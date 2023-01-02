package DominanceQueries

import scala.collection.mutable.ArrayBuffer

object Tools {

    def log(msg:String){
        val id = ">>>> " //Used to identify the logging that comes from our code
        print(Console.BLUE+" "+id+Console.GREEN+" "+msg+"\n")
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
                    if (point.dominates(points(j))){
                        points -= points(j) //Point is dominated, it's not a skyline point
                        j -= 1
                    }
                    else if (points(j).dominates(point)) {
                        j = points.length
                        skylinePoint = false //The given point is dominated, don't keep it...
                    }
                    j+=1
                    loop = j < points.length
                }
                if (skylinePoint) points += point
                i = i + 1
                this
            }
        }
        def print():Unit = {
            log("Skyline Points :")
            for(e <- points){
                log(e.getString())
            }
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
            def getString():String = {
                this.coordinates.mkString(",")+" (sum:"+sum.toString()+")"
            }
        }
}
