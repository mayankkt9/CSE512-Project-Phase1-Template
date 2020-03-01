package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
  def ST_Within(pointString1:String, pointString2:String, distance:Double) : Boolean = {
  try {
        val pointOne = pointString1.split(",")
        val pointTwo = pointString2.split(",")
        val pointOneX = pointOne(0).trim().toDouble
        val pointOneY = pointOne(1).trim().toDouble
        val pointTwoX = pointTwo(0).trim().toDouble
        val pointTwoY = pointTwo(1).trim().toDouble
        val distanceX = pointOneX - pointTwoX
        val distanceY = pointOneY - pointTwoY
        val calculatedDistance = scala.math.sqrt(scala.math.pow(distanceY, 2) + scala.math.pow(distanceX, 2))
        return calculatedDistance <= distance
    } 
  catch {
        case _: Throwable => return false
    }
}

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {

    try{
        val point = pointString.split(",")
        val rectangle = queryRectangle.split(",")
        val point_x = point(0).trim().toDouble
        val point_y = point(1).trim().toDouble
        val rec_x1 = rectangle(0).trim().toDouble
        val rec_y1 = rectangle(1).trim().toDouble
        val rec_x2 = rectangle(2).trim().toDouble
        val rec_y2 = rectangle(3).trim().toDouble
        val low_x = scala.math.min(rec_x1, rec_x2)
        val high_x = scala.math.max(rec_x1, rec_x2)
        val low_y = scala.math.min(rec_y1, rec_y2)
        val high_y = scala.math.max(rec_y1, rec_y2)

        if(point_y >= low_y && point_y <= high_y && point_x >= low_x && point_x <= high_x){
            return true
        }
        else{
            return false
        }
    }
    catch {
        case _: Throwable => return false
    }

    }

}
