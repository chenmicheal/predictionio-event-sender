package com.anarbaghirov.movielens

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, regexp_extract}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType}

object MovieLens {
  private implicit def sparSessionToSS(s: SparkSession): SparkSessionImprovements = new SparkSessionImprovements(s)

  def movies(sparkSession: SparkSession): sql.DataFrame = {
    import sparkSession.implicits._

    val file = sparkSession.loadCSV(MovieLensFiles.movies)
    val arrayContainsNull = true
    val movies = file
      .select(
        $"movieId".as("id").cast(IntegerType),
        $"title".as("name").cast(StringType),
        split($"genres", "~").as("genres").cast(ArrayType(StringType, arrayContainsNull)),
        regexp_extract(regexp_extract($"title", "(?:\\((\\d{4})\\))?\\s*$", 0), "\\b\\d{4}\\b", 0)
          .as("year").cast(StringType)
      )

    movies
  }

  def ratings(sparkSession: SparkSession): sql.DataFrame = {
    import sparkSession.implicits._

    val file = sparkSession.loadCSV(MovieLensFiles.ratings)
    val ratings = file
      .select(
        $"userId".as("userId").cast(IntegerType),
        $"movieId".as("movieId").cast(IntegerType),
        $"rating".as("rating").cast(DoubleType),
        $"timestamp".as("timestamp").cast(StringType)
      )

    ratings
  }
}
