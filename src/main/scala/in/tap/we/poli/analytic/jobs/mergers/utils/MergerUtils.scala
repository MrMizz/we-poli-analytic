package in.tap.we.poli.analytic.jobs.mergers.utils

object MergerUtils {

  def getMostCommon[A](seq: Seq[A]): Option[A] = {
    seq match {
      case Nil   => None
      case neSeq => Some(neSeq.groupBy(identity).maxBy(_._2.size)._1)
    }
  }

}
