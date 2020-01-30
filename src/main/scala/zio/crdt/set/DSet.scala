package zio.crdt.set

import zio.{UIO, ZIO}
import zio.Ref
import zio.crdt.VectorClock

sealed trait DSet[A] { self =>

  def union(set: DSet[A]): UIO[DSet[A]] =
    for {
      initState <- DSet.TwoSetState(self, set)(_ ++ _)
      stateRef <- Ref.make(initState)
    } yield {
      new DSet[A] {
        override def getState: UIO[DSet.State[A]] =
          for {
            state1 <- self.getState
            state2 <- set.getState
            isUpdated <- stateRef.get.map(
              s =>
                s.offset1 < state1.deltaOps.size || s.offset2 < state2.deltaOps.size
            )
            r <- if (isUpdated) stateRef.update { s =>
              def deltaOpHandler
                  : (DSet.TwoSetState[A], DeltaOp[A]) => DSet.TwoSetState[A] = {
                case (acc, DeltaOp.Added(elem))
                    if state1.set(elem) || state2.set(elem) && !acc.set(elem) =>
                  acc.copy(
                    set = acc.set + elem,
                    deltaOps = acc.deltaOps :+ DeltaOp.Added(elem)
                  )
                case (acc, DeltaOp.Removed(elem))
                    if !state1.set(elem) && !state2.set(elem) && acc
                      .set(elem) =>
                  acc.copy(
                    set = acc.set - elem,
                    deltaOps = acc.deltaOps :+ DeltaOp.Removed(elem)
                  )
                case (acc, _) => acc
              }

              val newState1 = (s.offset1 until state1.deltaOps.size)
                .map(i => state1.deltaOps(i))
                .foldLeft(s)(deltaOpHandler)

              (s.offset2 until state2.deltaOps.size)
                .map(i => state2.deltaOps(i))
                .foldLeft(newState1)(deltaOpHandler)
                .copy(
                  offset1 = state1.deltaOps.size,
                  offset2 = state2.deltaOps.size
                )
            } else stateRef.get
          } yield r
      }
    }

  def intersect(set: DSet[A]): UIO[DSet[A]] =
    for {
      initState <- DSet.TwoSetState(self, set)(_.intersect(_))
      stateRef <- Ref.make(initState)
    } yield {
      new DSet[A] {
        override def getState: UIO[DSet.State[A]] =
          for {
            state1 <- self.getState
            state2 <- set.getState
            isUpdated <- stateRef.get.map(
              s =>
                s.offset1 < state1.deltaOps.size || s.offset2 < state2.deltaOps.size
            )
            r <- if (isUpdated) stateRef.update { s =>
              def deltaOpHandler
                  : (DSet.TwoSetState[A], DeltaOp[A]) => DSet.TwoSetState[A] = {
                case (acc, DeltaOp.Added(elem))
                    if state1.set(elem) && state2.set(elem) && !acc.set(elem) =>
                  acc.copy(
                    set = acc.set + elem,
                    deltaOps = acc.deltaOps :+ DeltaOp.Added(elem)
                  )
                case (acc, DeltaOp.Removed(elem))
                    if !state1
                      .set(elem) || !state2.set(elem) && acc.set(elem) =>
                  acc.copy(
                    set = acc.set - elem,
                    deltaOps = acc.deltaOps :+ DeltaOp.Removed(elem)
                  )
                case (acc, _) => acc
              }

              val newState1 = (s.offset1 until state1.deltaOps.size)
                .map(i => state1.deltaOps(i))
                .foldLeft(s)(deltaOpHandler)

              (s.offset2 until state2.deltaOps.size)
                .map(i => state2.deltaOps(i))
                .foldLeft(newState1)(deltaOpHandler)
                .copy(
                  offset1 = state1.deltaOps.size,
                  offset2 = state2.deltaOps.size
                )
            } else stateRef.get
          } yield r
      }
    }

  def filter(p: A => Boolean): UIO[DSet[A]] =
    for {
      initState <- DSet.SingleSetState(self)(_.filter(p))
      stateRef <- Ref.make(initState)
    } yield {
      new DSet[A] {
        override def getState: UIO[DSet.State[A]] =
          for {
            state <- self.getState
            isUpdated <- stateRef.get.map(s => s.offset < state.deltaOps.size)
            r <- if (isUpdated) stateRef.update { s =>
              (s.offset until state.deltaOps.size)
                .map(i => state.deltaOps(i))
                .foldLeft(s) {
                  case (acc, DeltaOp.Added(elem))
                      if state.set(elem) && p(elem) && !acc.set(elem) =>
                    acc.copy(
                      set = acc.set + elem,
                      deltaOps = acc.deltaOps :+ DeltaOp.Added(elem)
                    )
                  case (acc, DeltaOp.Removed(elem))
                      if !state.set(elem) && acc.set(elem) =>
                    acc.copy(
                      set = acc.set - elem,
                      deltaOps = acc.deltaOps :+ DeltaOp.Removed(elem)
                    )
                  case (acc, _) => acc
                }
                .copy(offset = state.deltaOps.size)
            } else stateRef.get
          } yield r
      }
    }

  def map[B](f: A => B): UIO[DSet[B]] =
    for {
      initState <- DSet.SingleSetState(self)(_.map(f))
      stateRef <- Ref.make(initState)
    } yield {
      new DSet[B] {
        override def getState: UIO[DSet.State[B]] =
          for {
            state <- self.getState
            isUpdated <- stateRef.get.map(s => s.offset < state.deltaOps.size)
            r <- if (isUpdated) stateRef.update { s =>
              (s.offset until state.deltaOps.size)
                .map(i => state.deltaOps(i))
                .foldLeft(s) {
                  case (acc, DeltaOp.Added(elem)) if state.set(elem) =>
                    val v = f(elem)
                    if (!acc.set(v))
                      acc.copy(
                        set = acc.set + v,
                        deltaOps = acc.deltaOps :+ DeltaOp.Added(v)
                      )
                    else acc
                  case (acc, DeltaOp.Removed(elem)) if !state.set(elem) =>
                    val v = f(elem)
                    if (acc.set(v))
                      acc.copy(
                        set = acc.set - v,
                        deltaOps = acc.deltaOps :+ DeltaOp.Removed(v)
                      )
                    else acc
                  case (acc, _) => acc
                }
                .copy(offset = state.deltaOps.size)
            } else stateRef.get
          } yield r
      }
    }

  def contains(elem: A): UIO[Boolean] = getState.map(_.set(elem))

  def toSet: UIO[Set[A]] = getState.map(_.set)

  private[set] def getState: UIO[DSet.State[A]]

}

object DSet {

  private[set] sealed abstract class State[A](
      val set: Set[A],
      val deltaOps: Vector[DeltaOp[A]]
  )

  private[set] case class SingleSetState[A](
      override val set: Set[A],
      override val deltaOps: Vector[DeltaOp[A]],
      offset: Int
  ) extends State(set, deltaOps)

  private[set] object SingleSetState {
    def apply[A, B](
        dSet: DSet[A]
    )(f: Set[A] => Set[B]): UIO[SingleSetState[B]] =
      dSet.getState.map(
        s => SingleSetState(f(s.set), Vector.empty, s.deltaOps.size)
      )
  }

  private[set] case class TwoSetState[A](
      override val set: Set[A],
      override val deltaOps: Vector[DeltaOp[A]],
      offset1: Int,
      offset2: Int
  ) extends State(set, deltaOps)

  private[set] object TwoSetState {
    def apply[A1, A2, B](dSet1: DSet[A1], dSet2: DSet[A2])(
        combine: (Set[A1], Set[A2]) => Set[B]
    ): UIO[TwoSetState[B]] =
      for {
        initState1 <- dSet1.getState
        initState2 <- dSet2.getState
      } yield DSet.TwoSetState(
        combine(initState1.set, initState2.set),
        Vector.empty,
        initState1.deltaOps.size,
        initState2.deltaOps.size
      )
  }

}

class ORSet[A, B](private val state: Ref[ORSet.State[A, B]]) extends DSet[A] {

  def add(element: A, node: B): UIO[Unit] =
    state.update { s =>
      val newVVector = s.versionVector.increment(node)
      val newDot = VectorClock(Map(node -> newVVector.at(node)))
      val newElementsMap = s.elementsMap + (element -> newDot)
      val newDeltaOps = s.deltaOps :+ DeltaOp.Added(element)
      s.copy(
        elementsMap = newElementsMap,
        versionVector = newVVector,
        deltaOps = newDeltaOps
      )
    } *> ZIO.unit

  def remove(element: A, node: B): UIO[Unit] =
    state.update { s =>
      val newElementsMap = s.elementsMap - element
      val newVVector = s.versionVector.increment(node)
      val newDeltaOps = s.deltaOps :+ DeltaOp.Removed(element)
      s.copy(
        elementsMap = newElementsMap,
        versionVector = newVVector,
        deltaOps = newDeltaOps
      )
    } *> ZIO.unit

  def merge(otherState: ORSet.State[A, B]): UIO[Unit] = {
    state.update { s =>
      val newState = otherState.elementsMap.foldLeft(s) {
        case (acc, (element, v1)) =>
          s.elementsMap.get(element) match {
            case Some(v2) =>
              acc.copy(
                elementsMap = acc.elementsMap + (element -> v1.merge(v2)),
                deltaOps = acc.deltaOps :+ DeltaOp.Added(element)
              )
            case None =>
              if (s.versionVector
                    .compareTo(v1) == VectorClock.Ordering.Greater) acc
              else
                acc.copy(
                  elementsMap = acc.elementsMap + (element -> v1),
                  deltaOps = acc.deltaOps :+ DeltaOp.Added(element)
                )
          }
      }
      s.elementsMap.foldLeft(newState) {
        case (acc, (element, v1)) =>
          if (!otherState.elementsMap.contains(element) && otherState.versionVector
                .compareTo(v1) == VectorClock.Ordering.Greater)
            acc.copy(
              elementsMap = acc.elementsMap - element,
              deltaOps = acc.deltaOps :+ DeltaOp.Removed(element)
            )
          else acc
      }
    } *> ZIO.unit
  }

  def getState: UIO[ORSet.State[A, B]] =
    state.get

}

object ORSet {
  case class State[A, B](
      elementsMap: Map[A, VectorClock[B]],
      versionVector: VectorClock[B],
      override val deltaOps: Vector[DeltaOp[A]]
  ) extends DSet.State(elementsMap.keySet, deltaOps)

  def make[A, B]: UIO[ORSet[A, B]] =
    Ref
      .make(State[A, B](Map.empty, VectorClock.zero, Vector.empty))
      .map(state => new ORSet(state))
}
