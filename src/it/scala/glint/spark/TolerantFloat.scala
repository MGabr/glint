package glint.spark

import breeze.linalg._

import org.scalactic.{Equality, TolerantNumerics}

trait TolerantFloat {

  implicit val tolerantFloatEq: Equality[Float] = TolerantNumerics.tolerantFloatEquality(0.0000001f)

  implicit val tolerantFloatArrayEq: Equality[Array[Float]] = new Equality[Array[Float]] {
    override def areEqual(a: Array[Float], b: Any): Boolean = b match {
      case br: Array[Float] =>
        a.length == br.length && a.zip(br).forall { case (ax, bx) => tolerantFloatEq.areEqual(ax, bx) }
      case brr: Array[_] => a.deep == brr.deep
      case _ => a == b
    }F
  }

  implicit val tolerantFloatVectorEq: Equality[Vector[Float]] = new Equality[Vector[Float]] {
    override def areEqual(a: Vector[Float], b: Any): Boolean = b match {
      case br: Vector[Float] => tolerantFloatArrayEq.areEqual(a.toArray, br.toArray)
      case _ => a == b
    }
  }

  implicit val tolerantFloatVectorArrayEq: Equality[Array[Vector[Float]]] = new Equality[Array[Vector[Float]]]{
    override def areEqual(a: Array[Vector[Float]], b: Any): Boolean = b match {
      case br: Array[Vector[Float]] =>
        a.length == br.length && a.zip(br).forall { case (ax, bx) => tolerantFloatVectorEq.areEqual(ax, bx) }
      case _ => a == b
    }
  }
}