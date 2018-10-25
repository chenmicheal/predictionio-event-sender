package com.anarbaghirov.movielens

import java.util
import org.apache.predictionio.sdk.java.Event

case class Events(ratings: util.List[util.List[Event]],
                  movies: util.List[util.List[Event]],
                  users: util.List[util.List[Event]])
