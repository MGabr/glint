package glint.util.hdfs

import glint.Word2VecArguments

/**
  * Metadata of a word2vec matrix
  * In combination with the matrix data it provides enough information to load it again
  */
case class Word2VecMatrixMetadata(vocabCns: Array[Int], args: Word2VecArguments, trainable: Boolean)
