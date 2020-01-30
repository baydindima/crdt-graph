package zio.crdt

import zio.crdt.VectorClock.Ordering

import scala.annotation.tailrec

case class VectorClock[A](value: Map[A, Long]) extends AnyVal {
  def increment(node: A): VectorClock[A] =
    VectorClock(value + (node -> (value.getOrElse(node, 0L) + 1L)))

  def at(node: A): Long =
    value.getOrElse(node, 0)

  def compareTo(other: VectorClock[A]): VectorClock.Ordering = {
    val iterator = value.keysIterator ++ other.value.keysIterator
      .filterNot(value.contains)

    @tailrec
    def loop(prevOrder: VectorClock.Ordering): VectorClock.Ordering =
      iterator.nextOption() match {
        case Some(n) =>
          (value.getOrElse(n, 0L), other.value.getOrElse(n, 0L)) match {
            case (v1, v2) if v1 == v2 => loop(prevOrder)
            case (v1, v2)
                if v1 > v2 && prevOrder != VectorClock.Ordering.Lower =>
              loop(VectorClock.Ordering.Greater)
            case (v1, v2)
                if v1 < v2 && prevOrder != VectorClock.Ordering.Greater =>
              loop(VectorClock.Ordering.Lower)
            case _ => Ordering.Concurrent
          }
        case None => prevOrder
      }

    loop(VectorClock.Ordering.Equal)
  }

  def merge(other: VectorClock[A]): VectorClock[A] = {
    VectorClock(
      other.value.foldLeft(value) {
        case (acc, (n1, v1)) =>
          acc + (n1 -> Math.max(value.getOrElse(n1, 0L), v1))
      }
    )
  }

}

object VectorClock {
  def zero[A]: VectorClock[A] = VectorClock[A](value = Map.empty)

  sealed trait Ordering

  object Ordering {
    case object Equal extends Ordering

    case object Lower extends Ordering

    case object Greater extends Ordering

    case object Concurrent extends Ordering
  }
}
