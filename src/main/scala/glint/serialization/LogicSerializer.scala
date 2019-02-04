package glint.serialization
import java.nio.ByteBuffer

import glint.messages.server.logic._

/**
  * A fast serializer for logic messages
  *
  * Internally this uses byte buffers for fast serialization and deserialization.
  */
class LogicSerializer extends GlintSerializer {

  override def identifier: Int = 13372

  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = {
    o match {
      case x: AcknowledgeReceipt =>
        buf.put(SerializationConstants.logicAcknowledgeReceipt)
        buf.putInt(x.id)

      case x: Forget =>
        buf.put(SerializationConstants.logicForget)
        buf.putInt(x.id)

      case x: GetUniqueID =>
        buf.put(SerializationConstants.logicGetUniqueID)

      case x: NotAcknowledgeReceipt =>
        buf.put(SerializationConstants.logicNotAcknowledge)
        buf.putInt(x.id)

      case x: UniqueID =>
        buf.put(SerializationConstants.logicUniqueID)
        buf.putInt(x.id)
    }
  }

  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
    val objectType = buf.get()

    objectType match {
      case SerializationConstants.logicAcknowledgeReceipt =>
        val id = buf.getInt()
        AcknowledgeReceipt(id)

      case SerializationConstants.logicForget =>
        val id = buf.getInt()
        Forget(id)

      case SerializationConstants.logicGetUniqueID =>
        GetUniqueID()

      case SerializationConstants.logicNotAcknowledge =>
        val id = buf.getInt()
        NotAcknowledgeReceipt(id)

      case SerializationConstants.logicUniqueID =>
        val id = buf.getInt()
        UniqueID(id)
    }
  }

}
