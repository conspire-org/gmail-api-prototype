package gmail

import scala.collection.JavaConverters._
import com.google.api.client.googleapis.auth.oauth2._
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json._
import com.google.api.client.http._
import com.google.api.client.googleapis.batch._
import com.google.api.services.gmail._
import com.google.api.services.gmail.model._
import com.google.api.client.googleapis.json._

import scala.concurrent._
import scala.concurrent.duration._

import akka.actor._
import akka.routing._
import akka.pattern.CircuitBreaker
import akka.pattern.pipe


class ListActor(service: Gmail, http: HttpTransport) extends Actor with Stash {
  
  import ListActor._
  import MessageActor._
  
  import context.dispatcher
  
  val stats = context.actorOf(Props(classOf[StatsCollector], System.nanoTime))
  
  val breaker = CircuitBreaker(context.system.scheduler,
    maxFailures = 5,
    callTimeout = 10 seconds,
    resetTimeout = 10 seconds
  ).onOpen(
    println("Opening message breaker")
  ).onClose(
    println("Closing message breaker")
  ).onHalfOpen(
    println("Half-opening message breaker")
  )  
  
  val workers = context.actorOf(RoundRobinPool(4).props(Props(classOf[MessageActor], service, stats, http, breaker)))
  
  var counter = 0
  
  var pastCounts = Set.empty[Int]
  
  def receive = {
    case GetList(pageToken) =>
      val request = service.users().messages.list("me").setMaxResults(100)
      pageToken.foreach(request.setPageToken)
      
      try {
        val result = breaker.withSyncCircuitBreaker(request.execute())
        val threads = result.getMessages.asScala.map(_.getId)
        threads.toList.grouped(50).foreach { group =>
          workers ! GetMessageHeaders(group, counter)
          //java.lang.Thread.sleep(10)
        }
        stats ! GotList(counter)
        counter += 1
        self ! GetList(Some(result.getNextPageToken()))
      } catch {
        case ex => 
          println("List request failed: " + ex.getMessage)
          context.system.scheduler.scheduleOnce(10 seconds, self, GetList(pageToken))
      }
      
    case GotFailedMessage(listCount) =>
      if (! pastCounts.contains(listCount)) {
        self ! Pause
        pastCounts += listCount
        context.system.scheduler.scheduleOnce(30 seconds, self, StartAgain)
      }
  }
    
}

object ListActor {
  
  case class GetList(pageToken: Option[String] = None)
  case class GotList(id: Int, timestamp: Long = System.nanoTime)
  case object Pause
  case object StartAgain
  
}

class MessageActor(service: Gmail, stats: ActorRef, http: HttpTransport, breaker: CircuitBreaker) extends Actor {
  
  import MessageActor._
  
  val retryQueue = collection.mutable.Queue.empty[(String, Int)]
  
  import context.dispatcher
  
  def receive = {
    case GetMessageHeaders(messageIds, id) =>
      val request = batchRequest
      
      var errorPrinted = false
      
      messageIds.foreach { messageId =>
        val p = Promise[GotMessages]()
        
        //breaker.withCircuitBreaker(p.future) pipeTo stats
        p.future pipeTo stats
        
        request.queue(
          service.
          users().
          messages.
          get("me", messageId).
          setFormat("metadata").
          //setFields("messages/payload/headers").
          buildHttpRequest(),
          classOf[Message],
          classOf[GoogleJsonErrorContainer],
          new BatchCallback[Message, GoogleJsonErrorContainer] {
            def onSuccess(thread: Message, rf: HttpHeaders) {
              //p.success(GotMessages(id, thread.getMessages.size()))
              p.success(GotMessages(id, 1))
            }
            
            def onFailure(e: GoogleJsonErrorContainer, rf: HttpHeaders) {
              if (! errorPrinted) {
                println("Request failed, returned status " + e.getError.getCode)
                println(e.getError.getMessage)
                errorPrinted = true
              }
              val fail = GotFailedMessage(id)
              stats ! fail
              context.parent ! fail
              breaker.withSyncCircuitBreaker(fail)
              context.system.scheduler.scheduleOnce(20 seconds, self, Retry(messageId, id))
            }
          }
        )
      }
      
      breaker.withSyncCircuitBreaker(request.execute())
      
    case Retry(messageId, batchId) =>
      retryQueue.enqueue((messageId, batchId))
      
      if (retryQueue.size > 15) {
        retryQueue.toList.groupBy(_._2).foreach { case (batchId, ids) =>
          self ! GetMessageHeaders(ids.map(_._1), batchId)
        }
        retryQueue.clear()
      }
        
      
  }
      
  
  def batchRequest = new BatchRequest(http, new HttpRequestInitializer {
    def initialize(request: HttpRequest) = { }
  })
  
}

object MessageActor {
  
  case class GetMessageHeaders(messageIds: List[String], id: Int)
  case class GotMessages(id: Int, size: Int, timestamp: Long = System.nanoTime)
  case class GotFailedMessage(id: Int) extends Throwable("failed message")
  case class Retry(messageId: String, batchId: Int)
  
}

class StatsCollector(startTimeNanos: Long) extends Actor {
  
  import StatsCollector._
  import MessageActor._
  import ListActor._
  
  // Map of ListCounts -> List of completion times per messsage with head being the start time
  var completed = Map.empty[Int, List[Long]]
  var totalMessages = 0
  var totalLists = 0
  
  val startTime = System.nanoTime
  
  def receive = {
    case GotList(id, timestamp) =>
      completed = completed + (id -> List(timestamp))
      totalLists += 1
    case GotMessages(id, size, timestamp) =>
      completed.get(id).foreach { counts =>
        completed = completed + (id -> (counts ::: List.fill(size)(timestamp)))
      }
      totalMessages += 1
    case PrintStats =>
      val elapsed = (System.nanoTime - startTime) / 1000000000
      println(s"Found $totalMessages total messages and $totalLists total batches in $elapsed seconds")
      val avgBatchTimes = completed.collect { 
        case (id, times) if times.size > 1 =>
          (times.tail.map(t => t - times.head).sum * 1.0) / times.tail.size
      }
      val avgAvgBatchTime: Double = 
        if (avgBatchTimes.size == 0)
          0.0
        else
          ((avgBatchTimes.sum * 1.0) / avgBatchTimes.size) / 1000000000
      
      println(s"Average batch time is $avgAvgBatchTime seconds")
      val avgMessagesPerMinute = ((totalMessages * 1.0) / ((System.nanoTime - startTimeNanos) / 1000000000)) * 60
      println(s"Average messages per minute is $avgMessagesPerMinute")
  }
  
  import context.dispatcher
  
  override def preStart = {
    context.system.scheduler.schedule(Duration.Zero, 10 seconds, self, PrintStats)
  }
  
}

object StatsCollector {
  
  case object PrintStats
  
}

object GmailAPI extends App {
  
  import ListActor._

  val httpTransport = new NetHttpTransport
  val jsonFactory = new JacksonFactory

  val cred = new GoogleCredential().setAccessToken("ya29.oAA8tf1ptRFdboEYIzZ_xB5L6yTDADQ6q-rObkQe2Si-JdirxhH9o2Eg")
  val service = new Gmail.Builder(httpTransport, jsonFactory, cred).setApplicationName("Conspire Analyzer").build()
  
  implicit val sys = ActorSystem()
  
  sys.actorOf(Props(classOf[ListActor], service, httpTransport)) ! GetList()

}
