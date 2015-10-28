package org.aksw.sdw

import java.io.{BufferedInputStream, FileInputStream}
import java.net.URL
import java.util.Date
import java.util.zip.GZIPInputStream

import akka.actor.ActorRef
import org.aksw.sdw.ModelDistributor.{FinalizeOutput, ModelOrg}
import org.openrdf.model.Statement
import org.openrdf.rio.ntriples.NTriplesParser
import org.openrdf.rio.{ParseErrorListener, RDFHandler}

/**
 * Created by Chile on 3/16/2015.
 */
class RdfFileReader() {

  private var inputStream : GZIPInputStream = null
  private var parser: NTriplesParser = null
  private var date: Date = new Date()
  private var modelDistributor : ActorRef = null
  private var pauseStrm: PauseInputStream = null
  private val handler = new FreebaseRdfHandler()

  def prepareReading(path: String, modelDistributor: ActorRef): PauseInputStream =
  {
    this.modelDistributor = modelDistributor
    //
    if(path.contains("://"))
      inputStream = new GZIPInputStream(new URL(path).openStream())
    else
      inputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(path)))
    parser = new NTriplesParser()
    parser.setRDFHandler(handler)
    parser.setParseErrorListener(new FbParseErrorListener)
    date = new Date()
    pauseStrm = new PauseInputStream()
    pauseStrm
  }

  def startReading(offset: Long = 0): Unit =
  {
    inputStream.skip(offset)
    parser.parse(inputStream, "egal")
  }

  class FreebaseRdfHandler extends RDFHandler
  {
    private var mille : ModelPart = null
    private var lines : Long = 0

    override def startRDF(): Unit =
    {
    }

    override def handleComment(s: String): Unit = ???

    override def handleStatement(statement: Statement): Unit =
    {
      if(lines % 10000 == 0) {
       if(mille != null)
       {
         mille.setStreamEndLocation(pauseStrm.getOffset)
         modelDistributor ! ModelOrg(mille)
       }
        mille = new ModelPart()
        mille.setStreamStartLocation(pauseStrm.getOffset)
        System.out.println(lines + " lines")
        System.out.println( (new Date().getTime() - date.getTime())/ (1000 * 60 ))
      }
      mille.add(statement)
      lines = lines+1
    }

    override def endRDF(): Unit =
    {
      modelDistributor ! ModelOrg(mille)
      modelDistributor ! FinalizeOutput()
    }

    override def handleNamespace(s: String, s1: String): Unit = ???
  }

  class FbParseErrorListener extends ParseErrorListener{
    override def warning(s: String, i: Int, i1: Int): Unit =
    {
      System.out.println("warning: " + s)
    }

    override def error(s: String, i: Int, i1: Int): Unit = System.out.println("error: " + s)

    override def fatalError(s: String, i: Int, i1: Int): Unit = System.out.println("fatal: " + s)
  }
}
