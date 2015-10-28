package org.aksw.sdw

import java.io.StringWriter

import akka.actor.{Actor, ActorRef, Props}
import org.aksw.sdw.ModelDistributor._
import org.openrdf.model.impl.URIImpl
import org.openrdf.model.{Literal, Resource, Statement}
import org.openrdf.rio.ntriples.NTriplesWriter

import scala.util.control.Breaks._

/**
 * Created by Chile on 3/17/2015.
 */
class ModelFilter(subj: String, pred: String, obj: String, lang: String, virtusosCollector: ActorRef) extends Actor{
  val subjList = if(subj != null && subj.startsWith("$")) ConfigImpl.filterInserts(subj) else null
  val predList = if(pred != null && pred.startsWith("$")) ConfigImpl.filterInserts(pred) else null
  val objList = if(obj != null && obj.startsWith("$")) ConfigImpl.filterInserts(obj) else null

  val subjUri = if(subj != null && subjList == null) new URIImpl(resolveNamespaces(subj)) else null
  val predUri = if(pred != null && predList == null) new URIImpl(resolveNamespaces(pred)) else null
  val objUri = if(obj != null && objList == null) new URIImpl(resolveNamespaces(obj)) else null

  val name = (if(subj != null) "subj_" + subj) + (if(pred != null) "_pred_" + pred).toString + (if(obj != null) "_obj_" + obj) + (if(lang != null) "_language_" + lang).toString
  val fileOutput = context.actorOf(Props(classOf[GzOutputCollector], ConfigImpl.outputDirectory + name.replace(':', '.') + ".ttl.gz"))
  var firstInsert = true

  context.parent ! FilterReady(self.path.toString)

  override def receive: Receive =
  {
    case ModelOrg(model) => {
      val m = model.filter(subjUri, predUri, objUri)

      if (m.size() > 0)
      {
        val sw = new StringWriter()
        val writer: NTriplesWriter = new NTriplesWriter(sw)
        val iter = m.iterator()
        writer.startRDF()
        if(firstInsert) {
          writer.handleComment("FreeBase extraction for filter: " + name)
          firstInsert = false
        }
        while (iter.hasNext)
        {
          breakable {
            val st: Statement = iter.next()
            if (subjList != null) {
              if (!subjList.contains(st.getSubject))
                break;
            }
            if (predList != null) {
              if (!predList.contains(st.getPredicate))
                break;
            }
            if (objList != null) {
              if (!objList.contains(st.getObject.asInstanceOf[Resource]))
                break;
            }
            if (lang != null && st.getObject != null) {
              st.getObject match {
                case q if q == classOf[Literal] =>
                  if (q.asInstanceOf[Literal].getLanguage != lang)
                    break;
                case _ =>
              }
            }
            writer.handleStatement(st)
          }
        }
        writer.endRDF()
        if(virtusosCollector != null)
          virtusosCollector ! InsertTtl(sw.toString)
        fileOutput ! InsertTtl(sw.toString)
      }
      sender ! JobDone(self.path.toString)
    }
    case FinalizeOutput() =>
    {
      fileOutput ! FinalizeOutput()
    }
    case _ =>  //TODO
  }

  def resolveNamespaces(uri: String) : String =
  {
    var u = uri
    ModelFilter.prefixes.keys.foreach(key =>
    {
      val ind = u.indexOf(':')
      if(ind >= 0 && u.substring(0, ind) == key)
        u = ModelFilter.prefixes(key) + u.substring(ind+1)
    })
    u
  }


}

object ModelFilter{
    val prefixes = Map("ns" -> "http://rdf.freebase.com/ns/",
      "key" -> "http://rdf.freebase.com/key/",
      "owl" -> "http://www.w3.org/2002/07/owl#",
      "rdfs" -> "http://www.w3.org/2000/01/rdf-schema#",
      "xsd" -> "http://www.w3.org/2001/XMLSchema#")
}

object TripleSource extends Enumeration {
  type TripleSource = Value
  val Subject, Predicate, Object = Value
}
