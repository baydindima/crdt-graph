package zio.crdt

import zio.{Ref, UIO, ZIO, ZManaged}

trait EventObservable[T] {

  def addListener(q: Listener[T]): ZManaged[Any, Nothing, Unit]

  def addEvent(event: T): UIO[Unit]

}

object EventObservable {
  def make[T]: UIO[EventObservable[T]] =
    for {
      state <- Ref.make(Set.empty[Listener[T]])
    } yield new EventObservable[T] {
      override def addListener(q: Listener[T]): ZManaged[Any, Nothing, Unit] =
        ZManaged.make(state.update(_ + q))(_ => state.update(_ - q)).as(())

      override def addEvent(event: T): UIO[Unit] = {
        state.get.flatMap(qs => ZIO.foreach_(qs)(_.addEvent(event)))
      }
    }
}
