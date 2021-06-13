package in.tap.we.poli.analytic.jobs

import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address

package object mergers {

  def getMostCommon[A](seq: Seq[A]): Option[A] = {
    seq match {
      case Nil   => None
      case neSeq => Some(neSeq.groupBy(identity).maxBy(_._2.size)._1)
    }
  }

  object SetFreq {

    def init[A](item: A): Map[A, Long] = {
      Map(item -> 1L)
    }

    def init(item: Address): Map[Address, Long] = {
      (item.street, item.alternate_street, item.city, item.zip_code, item.state) match {
        case (None, None, None, None, None) =>
          Map.empty[Address, Long]
        case _ =>
          Map(item -> 1L)
      }
    }

    def put[A](item: A)(map: Map[A, Long]): Map[A, Long] = {
      map.get(item) match {
        case Some(freq) => map.updated(item, freq + 1L)
        case None       => map.updated(item, 1L)
      }
    }

    def getMostCommon[A](map: Map[A, Long]): Option[A] = {
      map.toList match {
        case Nil         => None
        case head :: Nil => Some(head._1)
        case nel =>
          Some(nel.maxBy(_._2)._1)
      }
    }

    def reduce[A](left: Map[A, Long], right: Map[A, Long]): Map[A, Long] = {
      right.toList match {
        case Nil => left
        case nel =>
          nel.foldLeft(left) {
            case (setFreq, (key, value)) =>
              setFreq.get(key) match {
                case Some(freq) => setFreq.updated(key, freq + value)
                case None       => setFreq.updated(key, value)
              }
          }
      }
    }

  }

}
