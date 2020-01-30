package zio.crdt.set

import zio.test.{DefaultRunnableSpec, suite, testM}
import zio.test._
import zio.test.Assertion._

object DSetSpec
    extends DefaultRunnableSpec(
      suite("DSetSpec")(
        testM("add elements to ORSet") {
          for {
            set1 <- ORSet.make[Int, String]
            _ <- set1.add(1, "a")
            r1 <- assertM(set1.contains(1), equalTo(true))
            r2 <- assertM(set1.contains(2), equalTo(false))
          } yield r1 && r2
        },
        testM("remove elements from ORSet") {
          for {
            set1 <- ORSet.make[Int, String]
            _ <- set1.add(1, "a")
            _ <- set1.remove(1, "a")
            r1 <- assertM(set1.contains(1), equalTo(false))
          } yield r1
        },
        testM("merge of ORSets") {
          for {
            set1 <- ORSet.make[Int, String]
            set2 <- ORSet.make[Int, String]
            _ <- set1.add(1, "a")
            _ <- set1.add(3, "a")

            _ <- set2.add(2, "b")
            _ <- set2.add(4, "b")
            set2State <- set2.getState
            _ <- set1.merge(set2State)
            r1 <- assertM(set1.contains(1), equalTo(true))
            r2 <- assertM(set1.contains(2), equalTo(true))
            r3 <- assertM(set1.contains(3), equalTo(true))
            r4 <- assertM(set1.contains(4), equalTo(true))
            r5 <- assertM(set1.contains(5), equalTo(false))
          } yield r1 && r2 && r3 && r4 && r5
        },
        testM("an addition of element should be a priority") {
          for {
            set1 <- ORSet.make[Int, String]
            set2 <- ORSet.make[Int, String]
            _ <- set1.add(1, "a")
            _ <- set2.remove(1, "b")
            set2State <- set2.getState
            _ <- set1.merge(set2State)
            r1 <- assertM(set1.contains(1), equalTo(true))
          } yield r1
        },
        testM("merge of ORSets with deletion") {
          for {
            set1 <- ORSet.make[Int, String]
            set2 <- ORSet.make[Int, String]
            _ <- set1.add(1, "a")
            _ <- set1.add(3, "a")

            _ <- set2.add(2, "b")
            _ <- set2.add(4, "b")

            _ <- set2.getState.flatMap(set1.merge)

            _ <- set2.remove(2, "b")
            _ <- set2.getState.flatMap(set1.merge)

            r1 <- assertM(set1.contains(1), equalTo(true))
            r2 <- assertM(set1.contains(2), equalTo(false))
            r3 <- assertM(set1.contains(3), equalTo(true))
            r4 <- assertM(set1.contains(4), equalTo(true))
            r5 <- assertM(set1.contains(5), equalTo(false))
          } yield r1 && r2 && r3 && r4 && r5
        },
        testM("create filter DSet") {
          for {
            set1 <- ORSet.make[Int, String]
            moreThan2 <- set1.filter(_ > 2)
            moreThan2AndOdd <- moreThan2.filter(_ % 2 != 0)
            _ <- set1.add(1, "a")
            _ <- set1.add(2, "a")
            _ <- set1.add(3, "a")
            _ <- set1.add(4, "a")
            r1 <- assertM(moreThan2.contains(1), equalTo(false))
            r2 <- assertM(moreThan2.contains(2), equalTo(false))
            r3 <- assertM(moreThan2.contains(3), equalTo(true))
            r4 <- assertM(moreThan2.contains(4), equalTo(true))
            r5 <- assertM(moreThan2.contains(5), equalTo(false))

            r6 <- assertM(moreThan2AndOdd.contains(1), equalTo(false))
            r7 <- assertM(moreThan2AndOdd.contains(2), equalTo(false))
            r8 <- assertM(moreThan2AndOdd.contains(3), equalTo(true))
            r9 <- assertM(moreThan2AndOdd.contains(4), equalTo(false))
            r10 <- assertM(moreThan2AndOdd.contains(5), equalTo(false))
          } yield r1 && r2 && r3 && r4 && r5 && r6 && r7 && r8 && r9 && r10
        },
        testM("create union DSet") {
          for {
            set1 <- ORSet.make[Int, String]
            set2 <- ORSet.make[Int, String]
            set3 <- ORSet.make[Int, String]

            union12 <- set1.union(set2)
            union13 <- set1.union(set3)
            union23 <- set2.union(set3)

            _ <- set1.add(1, "a")
            _ <- set2.add(2, "a")
            _ <- set3.add(3, "a")

            union123 <- union12.union(union13)

            r1 <- assertM(union12.contains(1), equalTo(true))
            r2 <- assertM(union12.contains(2), equalTo(true))
            r3 <- assertM(union12.contains(3), equalTo(false))

            r4 <- assertM(union13.contains(1), equalTo(true))
            r5 <- assertM(union13.contains(2), equalTo(false))
            r6 <- assertM(union13.contains(3), equalTo(true))

            r7 <- assertM(union23.contains(1), equalTo(false))
            r8 <- assertM(union23.contains(2), equalTo(true))
            r9 <- assertM(union23.contains(3), equalTo(true))

            r10 <- assertM(union123.contains(1), equalTo(true))
            r11 <- assertM(union123.contains(2), equalTo(true))
            r12 <- assertM(union123.contains(3), equalTo(true))

          } yield r1 && r2 && r3 && r4 && r5 && r6 && r7 && r8 && r9 && r10 && r11 && r12
        },
        testM("create intersect DSet") {
          for {
            set1 <- ORSet.make[Int, String]
            set2 <- ORSet.make[Int, String]

            intersectSet <- set1.intersect(set2)

            _ <- set1.add(1, "a")
            _ <- set1.add(2, "a")

            _ <- set2.add(2, "a")
            _ <- set2.add(3, "a")

            r1 <- assertM(intersectSet.contains(1), equalTo(false))
            r2 <- assertM(intersectSet.contains(2), equalTo(true))
            r3 <- assertM(intersectSet.contains(3), equalTo(false))
          } yield r1 && r2 && r3
        }
      )
    )
