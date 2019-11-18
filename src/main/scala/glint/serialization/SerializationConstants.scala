package glint.serialization

import sun.misc.Unsafe

/**
  * Some constants used for serialization
  */
private[glint] object SerializationConstants {

  // Byte identifiers for different request/response types
  val pullMatrixByte: Byte = 0x00
  val pullMatrixRowsByte: Byte = 0x01
  val pullVectorByte: Byte = 0x02

  val pushMatrixDoubleByte: Byte = 0x03
  val pushMatrixFloatByte: Byte = 0x04
  val pushMatrixIntByte: Byte = 0x05
  val pushMatrixLongByte: Byte = 0x06
  val pushVectorDoubleByte: Byte = 0x07
  val pushVectorFloatByte: Byte = 0x08
  val pushVectorIntByte: Byte = 0x09
  val pushVectorLongByte: Byte = 0x0A

  val pullDotProdByte: Byte = 0x0B
  val pushAdjustByte: Byte = 0x0C
  val pullMultiplyByte: Byte = 0x0D
  val pullNormDotsByte: Byte = 0x0E
  val pullAverageRowsByte: Byte = 0x0F

  val pullDotProdFMByte: Byte = 0x10
  val pullSumFMByte: Byte = 0x11
  val pushAdjustFMByte: Byte = 0x12

  val responseDoubleByte: Byte = 0x20
  val responseFloatByte: Byte = 0x21
  val responseIntByte: Byte = 0x22
  val responseLongByte: Byte = 0x23

  val responseDotProdByte: Byte = 0x24

  val responseDotProdFMByte: Byte = 0x25
  val responsePullSumFMByte: Byte = 0x26

  // Byte identifiers for message types
  val masterClientList: Byte = 0x00
  val masterRegisterClient: Byte = 0x01
  val masterRegisterServer: Byte = 0x02
  val masterServerList: Byte = 0x03

  val logicAcknowledgeReceipt: Byte = 0x04
  val logicForget: Byte = 0x05
  val logicGetUniqueID: Byte = 0x06
  val logicNotAcknowledge: Byte = 0x07
  val logicUniqueID: Byte = 0x08

  // Size of different java primitives to perform direct read/write to java memory
  val sizeOfByte = 1
  val sizeOfShort = 2
  val sizeOfInt = 4
  val sizeOfLong = 8
  val sizeOfFloat = 4
  val sizeOfDouble = 8

}
