import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.anarbaghirov.movielens._
import org.apache.spark.sql

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.FATAL)

    val (movies, ratings) = this.loadMoviesAndRatings()
    val url = "http://localhost:7070"
    val token = "QRaEzksG99fcakfU0X2j-sMGTYluq0SGAjFHImfyXk-zVNr06RvhQ-Zx07s0VMPl"
    val predictionIOClient = new PredictionIOClient(url, token)

    //predictionIOClient.sendMovies(movies)
    predictionIOClient.sendUsers(ratings)
    //predictionIOClient.sendRatings(ratings.limit(20000))

    predictionIOClient.close()
  }

  def loadMoviesAndRatings(): (sql.DataFrame, sql.DataFrame) = {
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
