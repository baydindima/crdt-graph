package zio.crdt

import zio.{Queue, Ref, UIO, ZIO}

trait Listener[T] {
  def addEvent(e: T): UIO[Unit]

  def setHandler(h: Handler[T]): UIO[Unit]
}

trait Handler[T] {
  def handle(event: T): UIO[Unit]
}

object Listener {
  def makeBounded[T](limit: Int): UIO[Listener[T]] =
    for {
      q <- Queue.unbounded[T]
      hRef <- Ref.make[Option[Handler[T]]](None)
    } yield new Listener[T] {
      override def addEvent(e: T): UIO[Unit] =
        for {
          _ <- q.offer(e)
          _ <- handleExcess
        } yield ()

      override def setHandler(h: Handler[T]): UIO[Unit] =
        hRef.update(_ => Some(h)) *> handleExcess

      private def handleExcess: ZIO[Any, Nothing, Unit] =
        ZIO.whenCaseM(hRef.get) {
          case Some(handler) =>
            ZIO.whenCaseM(q.size) {
              case qSize if qSize >= limit => q.takeUpTo(qSize - limit + 1).flatMap(elems => ZIO.foreach_(elems)(handler.handle))
            }
        }
    }
}