package org.aksw.sdw

import akka.actor.{ActorSystem, Props}
import org.aksw.sdw.ModelDistributor.SetStreamPauser

/**
 * Created by Chile on 3/18/2015.
 */
object Main {
  def main(args: Array[String]) {
    assert((args.length > 0))
    val actorSystem = ActorSystem()

    new ConfigImpl(args(0))
    val rdfReader = new RdfFileReader()
    val readerController = actorSystem.actorOf(Props(classOf[StreamControlActor], rdfReader))
    val modelDistributor = actorSystem.actorOf(Props(classOf[ModelDistributor], readerController))
    val pauseStream = rdfReader.prepareReading(ConfigImpl.inputFile, modelDistributor)
    val streamPauser = ActorSystem().actorOf(Props(classOf[StreamPauser], pauseStream))
      modelDistributor ! SetStreamPauser(streamPauser)  //set pauseInputStream

  }
}
