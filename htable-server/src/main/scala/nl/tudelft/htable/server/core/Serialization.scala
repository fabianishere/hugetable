package nl.tudelft.htable.server.core

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.InetSocketAddress

/**
 * Utilities for serializing objects between tablet servers.
 */
private[htable] object Serialization {

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
}
