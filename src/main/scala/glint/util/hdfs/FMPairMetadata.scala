package glint.util.hdfs

import glint.FMPairArguments

/**
  * Metadata of a FM-Pair matrix
  * In combination with the matrix data it provides enough information to load it again
  */
case class FMPairMetadata(args: FMPairArguments, featureProbs: Array[Float], avgActiveFeatures: Int, trainable: Boolean)
