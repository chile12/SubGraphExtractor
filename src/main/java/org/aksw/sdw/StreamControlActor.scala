package org.aksw.sdw

import akka.actor.Actor
import org.aksw.sdw.ModelDistributor.Start


/**
 * Created by Chile on 3/25/2015.
 */
class StreamControlActor(reader: RdfFileReader) extends Actor{
  override def receive: Receive =
  {
    case Start() =>
    {
      reader.startReading()
    }
  }
}
