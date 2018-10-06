package com.anarbaghirov.movielens

import org.apache.predictionio.sdk.java.EventClient
import org.apache.spark.sql

class PredictionIOClient(val url: String, token: String) {
  private implicit def eventClientToEC(c: EventClient): EventClientImprovements = new EventClientImprovements(c)
  private val eventClient = new EventClient(token, url)

  def close(): Unit = {
    this.eventClient.close()
  }

  def sendRatings(ratings: sql.DataFrame): Unit = {
    var count = 0

    ratings.collect().foreach(rating => {
      val userId = rating.getInt(0).toString
      val movieName = rating.getString(1)
      val ratingValue = rating.getDouble(2)

      this.eventClient.sendRating(userId, movieName, ratingValue)

      if (count % 200 == 0) {
        println(s"$userId voted $movieName with $ratingValue and count is $count")
      }

      count = count + 1
    })
  }

  def sendMovies(movies: sql.DataFrame): Unit = {
    movies.collect().foreach(movie => {
      val movieId = movie.getInt(0).toString
      val movieName = movie.getString(1)
      val genres = movie.getAs[Seq[String]](2).toArray
      val year = movie.getString(3)

      this.eventClient.sendMovie(movieName, movieName, genres, year)

      println(s"Event has been set for $movieId with $movieName and $genres")
    })
  }

  def sendUsers(ratings: sql.DataFrame): Unit = {
    val users = ratings.groupBy(ratings.col("userId")).count()

    users.collect().foreach(row => {
      val userId = row.getInt(0).toString

      this.eventClient.sendUser(userId)
      println(s"Event has been set for user $userId")
    })
  }
}
