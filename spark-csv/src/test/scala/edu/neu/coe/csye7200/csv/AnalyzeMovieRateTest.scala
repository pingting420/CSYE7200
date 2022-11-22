package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.{SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow

import scala.io.Source
import scala.util.Try

class MovieDataTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("Analyze_Movie_Rate")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  behavior of "Spark"

  //Show the data of movie first
  it should "Here is the data of Movie" taggedAs Slow in {
    val mdy = Try(Source.fromResource("movie_metadata.csv"))
    mdy.isSuccess shouldBe true
  }

  //If our program is correct, it should pass the test below
  it should "The result of the mean for all movies" taggedAs Slow in {
     AnalyzeMovieRate_Obj.AnalyzeMovieRating_Mean() should matchPattern {
      case 6.453200745804848=>
    }
  }

  it should "The result of the SD for all movies" taggedAs Slow in {
     AnalyzeMovieRate_Obj.AnalyzeMovieRating_SD() should matchPattern {
      case 0.9988071293753289=>
    }
  }
}