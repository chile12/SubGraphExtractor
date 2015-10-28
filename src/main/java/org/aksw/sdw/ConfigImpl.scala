package org.aksw.sdw

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import org.openrdf.model.impl.TreeModel
import org.openrdf.model.{Model, Resource}
import org.openrdf.rio.RDFHandler
import org.openrdf.rio.ntriples.NTriplesParser

import scala.collection.{JavaConversions, immutable}
import scala.util.parsing.json.JSON



/**
 * Created by Chile on 3/18/2015.
 */
class ConfigImpl(path: String) {

  private val source = scala.io.Source.fromFile(path)
  private val jsonString = source.mkString
  source.close()

  val zz = JSON.parseFull(jsonString)
  private val zw : Map[String, Any] = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]].get("map").get.asInstanceOf[Map[String, Any]]

  ConfigImpl.vHost = zw.get("virtuosoHost").get.toString
  ConfigImpl.vPort = zw.get("virtuosoPort").get.asInstanceOf[Double].toInt
  ConfigImpl.vUser = zw.get("virtuosoUser").get.toString
  ConfigImpl.vPass = zw.get("virtuosoPass").get.toString
  ConfigImpl.inputFile = zw.get("inputFile").get.toString
  ConfigImpl.outputDirectory = zw.get("baseDirectory").get.toString
  ConfigImpl.graphName = zw.get("graphName").get.toString
  ConfigImpl.filter = zw.get("filter").get.asInstanceOf[List[List[String]]]
  ConfigImpl.inserts = zw.get("filterInserts").get.asInstanceOf[Map[String, List[String]]]

  for(key <- ConfigImpl.inserts .keys)
  {
    ConfigImpl.filterInserts(key) = ConfigImpl.loadResourceList(key)
  }
}

object ConfigImpl{
  var vHost: String = null
  var vPort: Integer = null
  var vUser: String = null
  var vPass: String = null
  var inputFile: String = null
  var outputDirectory: String = null
  var graphName: String = null
  var filter: List[List[String]] = null
  var inserts : Map[String, List[String]]= null
  var filterInserts :  scala.collection.mutable.Map[String, Set[Resource]]=  scala.collection.mutable.Map()


  def loadResourceList(varibale: String) : immutable.Set[Resource] =
  {
    val set = new java.util.ArrayList[Resource]()
    val parser = new NTriplesParser()
    val handler = new FilterRdfHandler()
    parser.setRDFHandler(handler)

    for(path <- inserts(varibale))
    {
      val inputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(outputDirectory + path)))
      parser.parse(inputStream, "egal")
      System.out.println(path)
    }
    JavaConversions.asScalaSet(handler.getModel().subjects()).toSet

  }

  class FilterRdfHandler extends RDFHandler
  {
    private var model : TreeModel = new TreeModel()

    override def startRDF(): Unit =    {  }

    override def handleComment(s: String): Unit =     {}

    override def handleStatement(statement: org.openrdf.model.Statement): Unit =
    {
      model.add(statement)
    }

    override def endRDF(): Unit =     {  }

    override def handleNamespace(s: String, s1: String): Unit = {}

    def getModel() : Model = model
  }
}
