package com.anarbaghirov.movielens

import java.util

import org.apache.predictionio.sdk.java.{Event, EventClient}

class EventClientImprovements(val client: EventClient) {
  def sendRating(userId: String, movieName: String, rating: java.lang.Double): Unit = {
    val properties = new util.HashMap[String, AnyRef]()

    val eventName: String = {
      if (rating < 3) "dislike"
      else if (rating == 3) "any"
      else "like"
    }
    val entityType = "user"
    val targetEntity = "item"
    val event = new Event()
      .event(eventName)
      .entityType(entityType)
      .entityId(userId)
      .targetEntityType(targetEntity)
      .targetEntityId(movieName)
      .properties(properties)

    client.createEvent(event)
  }

  def sendMovie(id: String, name: String, genres: Array[String], year: String): Unit = {
    val properties = new util.HashMap[String, AnyRef]()

    properties.put("name", name)
    properties.put("genres", genres)
    properties.put("year", year)

    val eventName = "$set"
    val entityType = "item"
    val event = new Event().event(eventName).entityType(entityType).entityId(id).properties(properties)

    client.createEvent(event)
  }

  def sendUser(id: String): Unit = {
    val eventName = "$set"
    val entityType = "user"
    val event = new Event().event(eventName).entityType(entityType).entityId(id)

    client.createEvent(event)
  }
}
