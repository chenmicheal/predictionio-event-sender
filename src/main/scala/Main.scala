import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.anarbaghirov.movielens._
import com.google.common.collect.Lists
import org.apache.spark.sql
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)

    val url = "INSERT_URL_HERE"
    val token = "INSERT_APP_TOKEN_HERE"
    val predictionIOClient = new PredictionIOClient(url, token)

//    this.sendSingleEvents(predictionIOClient)
//    this.sendBatchEvents(predictionIOClient)

    predictionIOClient.close()
  }

  def sendBatchEvents(client: PredictionIOClient): Unit = {
    val events = this.loadMoviesAndRatingsEvents()

    events.ratings foreach { rating => client createEvents rating }
    events.movies foreach { movies => client createEvents movies }
    events.users foreach { users => client createEvents users }
  }

  def sendSingleEvents(client: PredictionIOClient): Unit = {
    val (movies, ratings) = this.loadMoviesAndRatings()

    client.sendMovies(movies)
    client.sendUsers(ratings)
    client.sendRatings(ratings.limit(20000))
  }

  private def loadMoviesAndRatingsEvents(): Events = {
    val (movies, ratings) = this.loadMoviesAndRatings()

    val ratingEvents = ratings.collect() map { r =>
      val userId = r.getInt(0).toString
      val movieName = r.getString(1)
      val ratingValue = r.getDouble(2)

      EventCreator.rating(userId, movieName, Predef.double2Double(ratingValue))
    }
    val ratingEventsPartitionedList = Lists.partition(ratingEvents.toList.asJava, 10000)

    val movieEvents = movies collect() map { movie =>
      val movieId = movie.getInt(0).toString
      val movieName = movie.getString(1)
      val genres = movie.getAs[Seq[String]](2).toArray
      val year = movie.getString(3)

      EventCreator.movie(movieId, movieName, genres, year)
    }
    val movieEventsPartitioned = Lists.partition(movieEvents.toList.asJava, 10000)

    val users = ratings.groupBy(ratings.col("userId")).count().collect() map { user =>
      val userId = user.getInt(0).toString

      EventCreator.user(userId)
    }
    val userEventsPartitioned = Lists.partition(users.toList.asJava, 10000)

    Events(ratingEventsPartitionedList, movieEventsPartitioned, userEventsPartitioned)
  }

  private def loadMoviesAndRatings(): (sql.DataFrame, sql.DataFrame) = {
    val sparkSession = SparkSession.builder().appName(name = "PIO").master(master = "local[*]").getOrCreate()
    val movies = MovieLens.movies(sparkSession).cache()
    val ratings = MovieLens.ratings(sparkSession).cache()
    val ratingsWithNames = ratings
      .join(movies, ratings.col("movieId") === movies.col("id"))
      .select(
        ratings.col(colName = "userId"),
        movies.col(colName = "name").as(alias = "movieName"),
        ratings.col(colName = "rating")
      ).cache()

    (movies, ratingsWithNames)
  }
}
