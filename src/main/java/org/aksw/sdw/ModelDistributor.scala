package org.aksw.sdw

import akka.actor.{Actor, ActorRef, Props}
import org.aksw.sdw.ModelDistributor._

import scala.collection.mutable.HashMap
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 3/17/2015.
 */
class ModelDistributor(reader: ActorRef) extends Actor{

  private var lockList : List[String] = List[String]()
  private val virtuosoCollector = if(ConfigImpl.vHost == null) null  else context.actorOf(Props(classOf[VirtuosoCollector], ConfigImpl.vHost, ConfigImpl.vPort, ConfigImpl.vUser, ConfigImpl.vPass, ConfigImpl.graphName))
  private val filterMap : ParHashMap[String, (ActorRef, Int, (String, String, String, String))] = new HashMap[String, (ActorRef, Int, (String, String, String, String))]().par
  private var streamPauser: ActorRef = null
  private var currentJobs =0

  ConfigImpl.filter.foreach(f => {
    val zw = context.actorOf(Props(classOf[ModelFilter], f(0), f(1), f(2), f(3), virtuosoCollector))
    context.watch(zw)
    lockList = lockList.:::(List(zw.path.toString()))
    filterMap += ((zw.path.toString(), (zw, 0, (f(0), f(1), f(2), f(3)))))
    })

  override def receive: Receive =
  {
    case ModelOrg(model) =>
    {
      currentJobs = currentJobs + filterMap.size
      System.out.println(currentJobs)

      for(x <-  filterMap)
      {
        x._2._1 ! ModelOrg(new ModelPart(model))
        if(x._2._2 +1 == ModelDistributor.filterQueueMax)
          lockList = lockList.:::(List(x._1))

        filterMap.update(x._1, x._2.copy(_2 = x._2._2 +1))
      }

      if(lockList.size > 0)
        streamPauser ! Pause(200)
    }
    case JobDone(msg) =>
    {
      currentJobs = currentJobs -1
      val zw = filterMap(msg)
      filterMap.update(msg, zw.copy(_2 = zw._2 -1))

      if(ModelDistributor.filterQueueMax == zw._2)
        lockList = lockList.filterNot(x => x == msg)

      if(lockList.size == 0)
        streamPauser ! UnPause()
    }
    case FilterReady(msg) =>
    {
      lockList = lockList.filterNot(x => x == msg)
      if(lockList.size == 0 && streamPauser != null)
        reader ! Start()
    }
    case UnPause() =>
    {
      if(lockList.size > 0)
        streamPauser ! Pause(200)
      else
        streamPauser ! UnPause()
    }
    case FinalizeOutput() =>
    {
      if(virtuosoCollector != null)
        virtuosoCollector ! FinalizeOutput()
      filterMap.values.foreach(x => x._1 ! FinalizeOutput())
      Thread.sleep(1000)
      System.exit(0)
    }
    case SetStreamPauser(pauser) =>
    {
      this.streamPauser = pauser
      self ! FilterReady("")
    }
    case _ =>   //TODO
  }
}

object ModelDistributor{
  val filterQueueMax = 10
  case class ModelOrg(model: ModelPart)
  case class JobDone(msg: String)
  case class FilterReady(msg: String)
  case class Pause(ms: Int)
  case class UnPause()
  case class Start()
  case class SetStreamPauser(pauser: ActorRef)
  case class InsertTtl(model: String)
  case class FinalizeOutput()
}
