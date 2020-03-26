package nl.tudelft.htable.client

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.net.InetSocketAddress

import akka.util.ByteString
import nl.tudelft.htable.protocol.client.{ReadRequest, RowSet}

import scala.language.implicitConversions

/**
 * Utilities for (de)serializing objects between tablet servers.
 */
private[htable] object SerializationUtils {
  /**
   * Convert the specified byte string into a socket address.
   */
  def deserialize(bytes: Array[Byte]): InetSocketAddress = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[InetSocketAddress]
  }

  /**
   * Convert the specified [Query] to a [ReadRequest].
   */
  def toReadRequest(query: Query): ReadRequest = {
    val rowSet = RowSet(
      rowKeys = query.rowKeys.map(buf => buf),
    )
    ReadRequest(
      tableName = query.table,
      rows = Some(rowSet),
      rowsLimit = query.limit
    )
  }

  /**
   * Translate an Akka ByteString into a Google Protobuf byte string.
   */
  implicit def akkaToProtobufByteString(byteString: ByteString): com.google.protobuf.ByteString =
    com.google.protobuf.ByteString.copyFrom(byteString.toByteBuffer)

  /**
   * Translate a Google Protobuf ByteString to Akka ByteString.
   */
  implicit def protobufToAkkaByteString(byteString: com.google.protobuf.ByteString): ByteString =
    ByteString(byteString.asReadOnlyByteBuffer())
}
