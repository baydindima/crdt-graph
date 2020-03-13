package zio.crdt

import zio.{Ref, UIO, ZManaged}

trait ORSet[T] extends DSet[T] {
  def add(elem: T, tag: ORSet.TagType): UIO[Unit]

  def remove(elem: T, tag: ORSet.TagType): UIO[Unit]

  def getState: UIO[Set[T]]

  def contains(elem: T): UIO[Boolean]

  def markVersion(version: ORSet.VersionType): UIO[Unit]

  def eraseVersion(version: ORSet.VersionType): UIO[Unit]

}

object ORSet {
  type TagType = Long
  type VersionType = Long

  def make[T]: UIO[ORSet[T]] =
    for {
      state <- Ref.make(State[T](Map.empty, Map.empty))
      eventObservable <- EventObservable.make[DeltaOp[T]]
    } yield new ORSet[T] {
      override def add(elem: T, tag: TagType): UIO[Unit] =
        state.update(s =>
          if (s.removeSet.get(elem).exists(_.contains(tag))) s else {
            val newAddSet = s.addSet + (elem -> (s.addSet.getOrElse(elem, Set.empty) + tag))
            s.copy(addSet = newAddSet)
          }
        ) *> eventObservable.addEvent(DeltaOp.Added(elem))

      override def remove(elem: T, tag: TagType): UIO[Unit] =
        state.update { s =>
          val newAddSet = s.addSet.get(elem) match {
            case Some(tags) if tags(tag) =>
              if (tags.size == 1) s.addSet - elem else s.addSet + (elem -> (tags - tag))
            case None => s.addSet
          }
          val newRemoveSet = if (s.removeSet.get(elem).exists(_.contains(tag))) s.removeSet else s.removeSet + (elem -> Map(tag -> None))
          s.copy(addSet = newAddSet, removeSet = newRemoveSet)
        } *> eventObservable.addEvent(DeltaOp.Removed(elem))

      override def getState: UIO[Set[T]] = state.get.map(_.addSet.keySet)

      override def contains(elem: T): UIO[Boolean] = getState.map(_.contains(elem))

      override def markVersion(version: VersionType): UIO[Unit] =
        state.update { s =>
          val newRemoveSet = s.removeSet.view.mapValues(tags =>
            tags.view.mapValues {
              case v@Some(_) => v
              case None => Some(version)
            }.toMap).toMap
          s.copy(removeSet = newRemoveSet)
        }.as(())

      override def eraseVersion(version: VersionType): UIO[Unit] =
        state.update { s =>
          val newRemoveSet = s.removeSet.view.mapValues(tags =>
            tags.filterNot(_._2.contains(version))
          ).filter(_._2.nonEmpty).toMap
          s.copy(removeSet = newRemoveSet)
        }.as(())

      override protected def addListener(): ZManaged[Any, Nothing, Listener[DeltaOp[T]]] =
        Listener.makeBounded[DeltaOp[T]](1024).toManaged_.flatMap(l => eventObservable.addListener(l).as(l))

      override protected def updateState(event: DeltaOp[T]): UIO[Unit] = throw new NotImplementedError("Shouldn't be called on ORSet")
    }

  private case class State[T](addSet: Map[T, Set[TagType]], removeSet: Map[T, Map[TagType, Option[VersionType]]])

}
