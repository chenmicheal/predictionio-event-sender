package com.anarbaghirov.movielens

import java.util
import org.apache.predictionio.sdk.java.Event

object EventCreator {
  def rating(userId: String, movieName: String, rating: java.lang.Double): Event = {
    val properties = new util.HashMap[String, AnyRef]()

    val eventName: String = {
      if (rating < 3) "dislike"
      else if (rating == 3) "any"
      else "like"
    }
    val entityType = "user"
    val targetEntity = "item"

    new Event()
      .event(eventName)
      .entityType(entityType)
      .entityId(userId)
      .targetEntityType(targetEntity)
      .targetEntityId(movieName)
      .properties(properties)
  }

  def movie(id: String, name: String, genres: Array[String], year: String): Event = {
    val properties = new util.HashMap[String, AnyRef]()

    properties.put("name", name)
    properties.put("genres", genres)
    properties.put("year", year)

    val eventName = "$set"
    val entityType = "item"

    new Event().event(eventName).entityType(entityType).entityId(id).properties(properties)
  }

  def user(id: String): Event = {
    val eventName = "$set"
    val entityType = "user"

    new Event().event(eventName).entityType(entityType).entityId(id)
  }
}
