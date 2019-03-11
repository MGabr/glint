package glint.util.hdfs

/**
  * Metadata of a word2vec matrix
  * In combination with the matrix data it provides enough information to load it again
  */
case class Word2VecMatrixMetadata(vocabCns: Array[Int],
                                  vectorSize: Int,
                                  window: Int,
                                  batchSize: Int,
                                  n: Int,
                                  unigramTableSize: Int,
                                  trainable: Boolean)
