package zio.crdt

import zio.UIO
import zio.test._
import zio.test.Assertion._

object VectorClockSpec
    extends DefaultRunnableSpec(
      suite("VectorClockSpec")(
        testM("VectorClock correctly increment node value") {
          val initVectorClock = VectorClock.zero[String]

          val node1 = "a"
          val node2 = "b"
          val node3 = "c"

          val increments = List(node1, node1, node2, node1, node2)

          val incrementedVectorClock = increments.foldLeft(initVectorClock) {
            case (acc, node) => acc.increment(node)
          }
          UIO.succeed(
            assert(incrementedVectorClock.at(node1), equalTo(3)) &&
              assert(incrementedVectorClock.at(node2), equalTo(2)) &&
              assert(incrementedVectorClock.at(node3), equalTo(0))
          )
        },
        testM("VectorClock correctly compare to another vector clock") {
          val node1 = "a"
          val node2 = "b"
          val node3 = "c"

          val vectorClock = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 3
            )
          )

          val vectorClockEq = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 3
            )
          )

          val vectorClockG1 = VectorClock(
            Map(
              node1 -> 2,
              node2 -> 3
            )
          )

          val vectorClockG2 = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 4
            )
          )

          val vectorClockG3 = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 3,
              node3 -> 1
            )
          )

          val vectorClockL1 = VectorClock.zero[String]

          val vectorClockL2 = VectorClock(
            Map(
              node2 -> 3
            )
          )

          val vectorClockL3 = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 2
            )
          )

          val vectorClockC2 = VectorClock(
            Map(
              node1 -> 2,
              node2 -> 2
            )
          )

          UIO.succeed(
            assert(
              vectorClock.compareTo(vectorClockEq),
              equalTo(VectorClock.Ordering.Equal)
            ) &&
              assert(
                vectorClock.compareTo(vectorClockG1),
                equalTo(VectorClock.Ordering.Lower)
              ) &&
              assert(
                vectorClock.compareTo(vectorClockG2),
                equalTo(VectorClock.Ordering.Lower)
              ) &&
              assert(
                vectorClock.compareTo(vectorClockG3),
                equalTo(VectorClock.Ordering.Lower)
              ) &&
              assert(
                vectorClock.compareTo(vectorClockL1),
                equalTo(VectorClock.Ordering.Greater)
              ) &&
              assert(
                vectorClock.compareTo(vectorClockL2),
                equalTo(VectorClock.Ordering.Greater)
              ) &&
              assert(
                vectorClock.compareTo(vectorClockL3),
                equalTo(VectorClock.Ordering.Greater)
              ) &&
              assert(
                vectorClock.compareTo(vectorClockC2),
                equalTo(VectorClock.Ordering.Concurrent)
              )
          )
        },
        testM("VectorClock correctly merge with other vector clock") {
          val node1 = "a"
          val node2 = "b"
          val node3 = "c"

          val vectorClock = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 3
            )
          )

          val vectorClock1 = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 3
            )
          )

          val vectorClock2 = VectorClock(
            Map(
              node1 -> 2,
              node2 -> 3
            )
          )

          val vectorClock3 = VectorClock(
            Map(
              node1 -> 1,
              node2 -> 2
            )
          )

          val vectorClock4 = VectorClock(
            Map(
              node1 -> 2,
              node2 -> 2,
              node3 -> 1
            )
          )

          UIO.succeed(
            assert(
              vectorClock.merge(vectorClock1),
              equalTo(
                VectorClock(
                  Map(
                    node1 -> 1,
                    node2 -> 3
                  )
                )
              )
            ) &&
              assert(
                vectorClock.merge(vectorClock2),
                equalTo(
                  VectorClock(
                    Map(
                      node1 -> 2,
                      node2 -> 3
                    )
                  )
                )
              ) &&
              assert(
                vectorClock.merge(vectorClock3),
                equalTo(
                  VectorClock(
                    Map(
                      node1 -> 1,
                      node2 -> 3
                    )
                  )
                )
              ) &&
              assert(
                vectorClock.merge(vectorClock4),
                equalTo(
                  VectorClock(
                    Map(
                      node1 -> 2,
                      node2 -> 3,
                      node3 -> 1
                    )
                  )
                )
              )
          )
        }
      )
    )
