package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

//Build the class for our program
class Analyze_Movie_Rate{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Analyze_Movie_Rate")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") 
  
  //Read the movie_metadata.csv file and create a val
  val path = "src/main/resources/movie_metadata.csv"

  val Movie_DataFrame = spark.read.option("delimiter", ",")
    .option("header", "true")
    .csv(path)

  //Analyze the mean of the Movies
  def AnalyzeMovieRating_Mean() = {
    val avge: DataFrame = Movie_DataFrame.select("movie_title", "imdb_score")
      .groupBy("movie_title") // use this way to calculate the mean for the movies and aggregated by moive's name
      .agg(functions.avg("imdb_score"))
    avge.show() 
    val mean = Movie_DataFrame.select(functions.avg("imdb_score"))
    mean.show() // to calculate the mean for all movies
    mean.collect()(0).get(0)
  }

  //Analyze the Standard Deviation of the Movies
  def AnalyzeMovieRating_SD(): Any = {
    val sdv = Movie_DataFrame.select(functions.stddev("imdb_score"))//Here we could use the function stddev to calculate the sd directly
    sdv.show() // get the resule
    sdv.collect()(0).get(0)
  }
}

object AnalyzeMovieRate_Obj extends Analyze_Movie_Rate
{
  def main(args:Array[String]) : Unit =
  {
    val data = new Analyze_Movie_Rate()
    data.AnalyzeMovieRating_Mean() //We get the resule of the mean 6.453200745804848
    data.AnalyzeMovieRating_SD() // We get the result of the SD 0.9988071293753289
  }
}