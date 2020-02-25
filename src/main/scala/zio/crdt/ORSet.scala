package zio.crdt

import zio.{Ref, UIO}

trait ORSet[T] {
  def add(elem: T, tag: ORSet.TagType): UIO[Unit]

  def remove(elem: T, tag: ORSet.TagType): UIO[Unit]

}

object ORSet {
  type TagType = Long
  type VersionType = Long

  def make[T]: UIO[ORSet[T]] =
    for {
      state <- Ref.make(State[T](Map.empty, Map.empty))
    } yield new ORSet[T] {
      override def add(elem: T, tag: TagType): UIO[Unit] =
        state.update(s =>
          if (s.removeSet.get(elem).exists(_.contains(tag))) s else {
            val newAddSet = s.addSet + (elem -> (s.addSet.getOrElse(elem, Set.empty) + tag))
            s.copy(addSet = newAddSet)
          }
        ).as(())

      override def remove(elem: T, tag: TagType): UIO[Unit] =
        state.update(s =>
          if (s.removeSet.get(elem).exists(_.contains(tag))) s else {
            val newAddSet = s.addSet + (elem -> (s.addSet.getOrElse(elem, Set.empty) + tag))
            s.copy(addSet = newAddSet)
          }
        ).as(())
    }

  private case class State[T](addSet: Map[T, Set[TagType]], removeSet: Map[T, Map[TagType, List[VersionType]]])

}
