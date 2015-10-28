package org.aksw.sdw

import akka.actor.Actor
import org.aksw.sdw.ModelDistributor.{Pause, UnPause}

/**
 * Created by Chile on 3/19/2015.
 */
class StreamPauser(stream: PauseInputStream) extends Actor
{
  private var setUnpause = false

  self ! Pause(200)

  override def receive: Receive =
  {
    case Pause(ms) => {
      if (stream != null) {
        stream.pause()
        Thread.sleep(ms)
        sender ! UnPause()
      }
    }
    case UnPause() =>
    {
      if (stream != null) {
        stream.unPause()
      }
      else
        setUnpause = true
    }
  }
}
