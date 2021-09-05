package in.tap.we.poli.analytic.jobs.dynamo.traversal.nx

import org.apache.spark.graphx.VertexId

trait NxKey {

  protected val keys: Seq[VertexId]

  def key: String = {
    keys.sorted.reduce {
      _ + "_" + _
    }
  }

}

object NxKey {

  final case class N1Key(
    src_id_1: VertexId
  ) extends NxKey {
    override protected val keys: Seq[VertexId] = Seq(src_id_1)
  }

  final case class N2Key(
    src_id_1: VertexId,
    src_id_2: VertexId
  ) extends NxKey {
    override protected val keys: Seq[VertexId] = Seq(src_id_1, src_id_2)
  }

  final case class N3Key(
    src_id_1: VertexId,
    src_id_2: VertexId,
    src_id_3: VertexId
  ) extends NxKey {
    override protected val keys: Seq[VertexId] = Seq(src_id_1, src_id_2, src_id_3)
  }

  final case class N4Key(
    src_id_1: VertexId,
    src_id_2: VertexId,
    src_id_3: VertexId,
    src_id_4: VertexId
  ) extends NxKey {
    override protected val keys: Seq[VertexId] = Seq(src_id_1, src_id_2, src_id_3, src_id_4)
  }

  final case class N5Key(
    src_id_1: VertexId,
    src_id_2: VertexId,
    src_id_3: VertexId,
    src_id_4: VertexId,
    src_id_5: VertexId
  ) extends NxKey {
    override protected val keys: Seq[VertexId] = Seq(src_id_1, src_id_2, src_id_3, src_id_4, src_id_5)
  }

}
