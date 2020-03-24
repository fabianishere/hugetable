package nl.tudelft.htable.server.core

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.locks.LockSupport

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import nl.tudelft.htable.protocol.{ClientService, ClientServiceHandler, ReadRequest, ReadResponse}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.recipes.nodes.GroupMember
import org.apache.zookeeper.CreateMode

import scala.concurrent.{ExecutionContext, Future}

/**
 * Main implementation of a tablet server as described in the Google BigTable paper.
 *
 * @param client The ZooKeeper client to use for synchronization.
 */
class HTableServer(private val client: CuratorFramework) extends Runnable {
  /**
   * The logger instance of this class.
   */
  private val log = Logger[HTableServer]

  /**
   * The [LeaderSelector] instance for performing a leader election via ZooKeeper.
   */
  private val leaderSelector = new LeaderSelector(client, "/leader", new LeaderSelectorListenerAdapter {
    override def takeLeadership(client: CuratorFramework): Unit = {
      log.info("I took Leadership!")
      Thread.sleep(10000)
    }
  })

  /**
   * The [GroupMember] instance for keeping track of the tablet servers.
   */
  private val membership = new GroupMember(client, "/servers", UUID.randomUUID().toString)

  /**
   * Run the main logic of the server.
   */
  override def run(): Unit = {
    log.info("Starting HugeTable server")

    log.info("Starting leader selection")
    // Ensure we re-enqueue when the instance relinquishes leadership
    leaderSelector.autoRequeue()
    leaderSelector.start()

    log.info("Requesting group membership")
    membership.start()

    log.info("Start public facing HTableService")
    startServer()

    // Register root tablet
    client.create()
      .withMode(CreateMode.EPHEMERAL)
      .forPath("/root", serialize(new InetSocketAddress("127.0.0.1", 8080)))

    try {
      // Park the current thread until interruption
      LockSupport.park()
    } finally {
      leaderSelector.close()
      membership.close()
    }
  }

  /**
   * Start the user facing [HTableService] instance.
   */
  private def startServer(): Unit = {
    // Important: enable HTTP/2 in ActorSystem's config
    // We do it here programmatically, but you can also set it in the application.conf
    val conf = ConfigFactory
      .parseString("akka.http.server.preview.enable-http2 = on")
      .withFallback(ConfigFactory.defaultApplication())
    val system = ActorSystem("HelloWorld", conf)

    // Akka boot up code
    implicit val sys: ActorSystem = system
    implicit val mat: Materializer = Materializer(sys)
    implicit val ec: ExecutionContext = sys.dispatcher

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      ClientServiceHandler(new ClientServiceImpl())

    // Bind service handler servers to localhost:8080/8081
    val binding = Http().bindAndHandleAsync(
      service,
      interface = "127.0.0.1",
      port = 8080,
      connectionContext = HttpConnectionContext())

    // report successful binding
    binding.foreach { binding => log.info(s"gRPC server bound to: ${binding.localAddress}") }
  }

  private def serialize(value: InetSocketAddress): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  class ClientServiceImpl(implicit mat: Materializer) extends ClientService {
    /**
     * Read the specified row (range) and stream back the response.
     */
    override def read(in: ReadRequest): Source[ReadResponse, NotUsed] = {
      log.info("Received READ request")
      Source.single(ReadResponse())
    }
  }
}
