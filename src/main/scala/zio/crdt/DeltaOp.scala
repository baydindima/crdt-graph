package zio.crdt

sealed trait DeltaOp[A]

object DeltaOp {
  case class Added[A](elem: A) extends DeltaOp[A]
  case class Removed[A](elem: A) extends DeltaOp[A]
}
