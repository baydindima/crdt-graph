package zio.crdt

import zio.{Ref, UIO, ZIO, ZManaged}
import ZIOBooleanOps._

// TODO check event relevancy ???  before updating a state for decreasing event queue size. Do not duplicate events and do not add already not relevant events
trait DSet[T] {

  def union(other: DSet[T]): ZManaged[Any, Nothing, DSet[T]] = {
    def unionSync(dSet: DSet[T], event: DeltaOp[T]): UIO[Unit] = event match {
      case e@DeltaOp.Added(elem) =>
        ZIO.whenM(this.contains(elem) || other.contains(elem))(dSet.updateState(e))
      case e@DeltaOp.Removed(elem) =>
        ZIO.whenM(this.contains(elem).invert && other.contains(elem).invert)(dSet.updateState(e))
    }

    DSet.createFrom2Sym(this, other, _.union(_), unionSync)
  }

  def intersect(other: DSet[T]): ZManaged[Any, Nothing, DSet[T]] = {
    def intersectSync(dSet: DSet[T], event: DeltaOp[T]): UIO[Unit] = event match {
      case e@DeltaOp.Added(elem) =>
        ZIO.whenM(this.contains(elem) && other.contains(elem))(dSet.updateState(e))
      case e@DeltaOp.Removed(elem) =>
        ZIO.whenM(this.contains(elem).invert || other.contains(elem).invert)(dSet.updateState(e))
    }

    DSet.createFrom2Sym(this, other, _.intersect(_), intersectSync)
  }

  def filter(p: T => Boolean): ZManaged[Any, Nothing, DSet[T]] = {
    def syncF(dSet: DSet[T], event: DeltaOp[T]): UIO[Unit] = event match {
      case e@DeltaOp.Added(elem) =>
        ZIO.when(p(elem))(dSet.updateState(e))
      case e@DeltaOp.Removed(_) =>
        dSet.updateState(e).as(())
    }

    for {
      listener <- this.addListener()
      initState <- getState.map(_.filter(p)).toManaged_
      dSet <- DSet.make[T](initState).toManaged_
      _ <- listener.setHandler((event: DeltaOp[T]) => syncF(dSet, event)).toManaged_
    } yield dSet
  }


  def contains(elem: T): UIO[Boolean] = getState.map(_.contains(elem))

  def getState: UIO[Set[T]]

  protected def addListener(): ZManaged[Any, Nothing, Listener[DeltaOp[T]]]

  protected def updateState(event: DeltaOp[T]): UIO[Unit]

}

object DSet {
  def make[T](initState: Set[T]): UIO[DSet[T]] =
    for {
      state <- Ref.make(initState)
      eventObservable <- EventObservable.make[DeltaOp[T]]
    } yield new DSet[T] {
      override def getState: UIO[Set[T]] =
        state.get

      override protected def addListener(): ZManaged[Any, Nothing, Listener[DeltaOp[T]]] =
        Listener.makeBounded[DeltaOp[T]](1024).toManaged_.flatMap(l => eventObservable.addListener(l).as(l))

      override protected def updateState(event: DeltaOp[T]): UIO[Unit] = (event match {
        case DeltaOp.Added(elem) =>
          state.update(_ + elem)
        case DeltaOp.Removed(elem) =>
          state.update(_ - elem)
      }) *> eventObservable.addEvent(event)
    }


  private def createFrom2Sym[T](dSet1: DSet[T], dSet2: DSet[T], initStateF: (Set[T], Set[T]) => Set[T], syncF: (DSet[T], DeltaOp[T]) => UIO[Unit]): ZManaged[Any, Nothing, DSet[T]] =
    for {
      listener1 <- dSet1.addListener()
      listener2 <- dSet2.addListener()
      state1 <- dSet1.getState.toManaged_
      state2 <- dSet2.getState.toManaged_
      initState = initStateF(state1, state2)
      dSet <- DSet.make[T] (initState).toManaged_
      _ <- listener1.setHandler((event: DeltaOp[T]) => syncF(dSet, event)).toManaged_
      _ <- listener2.setHandler((event: DeltaOp[T]) => syncF(dSet, event)).toManaged_
    } yield dSet

}