package nl.tudelft.htable.protocol

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress

import akka.util.ByteString
import nl.tudelft.htable.core.Query
import nl.tudelft.htable.protocol.client.{ReadRequest, RowRange, RowSet}

import scala.language.implicitConversions

/**
 * Utilities for (de)serializing objects between tablet servers.
 */
private[htable] object SerializationUtils {
  /**
   * Serialize an [InetSocketAddress] to a byte string.
   */
  def serialize(value: InetSocketAddress): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

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
      rowRanges = query.ranges.map(range => RowRange(startKey = range.start, endKey = range.end))
    )
    ReadRequest(
      tableName = query.table,
      rows = Some(rowSet),
      rowsLimit = query.limit
    )
  }

  /**
   * Translate an Akka byte string into a Google Protobuf byte string.
   */
  implicit def akkaToProtobuf(byteString: ByteString): com.google.protobuf.ByteString =
    com.google.protobuf.ByteString.copyFrom(byteString.toByteBuffer)

  /**
   * Translate a Google Protobuf ByteString to Akka ByteString.
   */
  implicit def protobufToAkka(byteString: com.google.protobuf.ByteString): ByteString =
    ByteString(byteString.asReadOnlyByteBuffer())
}
