package zio

package object crdt {

  object ZIOBooleanOps {
    implicit class ZBooleanOps[R1, E](zBool: ZIO[R1, E, Boolean]) {
      def &&[R2](zBool2: ZIO[R2, E, Boolean]): ZIO[R2 with R1, E, Boolean] = zBool.flatMap(b => if (b) zBool2 else ZIO.succeed(false))
      def ||[R2](zBool2: ZIO[R2, E, Boolean]): ZIO[R2 with R1, E, Boolean] = zBool.flatMap(b => if (b) ZIO.succeed(true) else zBool2)

      def invert: ZIO[R1, E, Boolean] = zBool.map(!_)
    }
  }

}
