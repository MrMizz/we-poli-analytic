package in.tap.we.poli.analytic.jobs.dynamo.traversal.nx

import org.apache.spark.graphx.VertexId

trait NxKey {

  val keys: Seq[VertexId]

  def key: String = {
    keys.sorted.map(_.toString).reduce {
      _ + "_" + _
    }
  }

}

object NxKey {

  final case class N1Key(
    src_id_1: VertexId
  ) extends NxKey {
    override val keys: Seq[VertexId] = Seq(src_id_1)
  }

  final case class N2Key(
    src_id_1: VertexId,
    src_id_2: VertexId
  ) extends NxKey {
    override val keys: Seq[VertexId] = Seq(src_id_1, src_id_2)
  }

  object N2Key {

    val apply: (N1Key, VertexId) => Option[N2Key] = {
      case (n1Key: N1Key, vertexId: VertexId) =>
        if (n1Key.keys.contains(vertexId)) {
          None
        } else {
          Some(
            N2Key(
              src_id_1 = n1Key.src_id_1,
              src_id_2 = vertexId
            )
          )
        }
    }

  }

  final case class N3Key(
    src_id_1: VertexId,
    src_id_2: VertexId,
    src_id_3: VertexId
  ) extends NxKey {
    override val keys: Seq[VertexId] = Seq(src_id_1, src_id_2, src_id_3)
  }

  object N3Key {

    val apply: (N2Key, VertexId) => Option[N3Key] = {
      case (n2Key: N2Key, vertexId: VertexId) =>
        if (n2Key.keys.contains(vertexId)) {
          None
        } else {
          Some(
            N3Key(
              src_id_1 = n2Key.src_id_1,
              src_id_2 = n2Key.src_id_2,
              src_id_3 = vertexId
            )
          )
        }
    }

  }

  final case class N4Key(
    src_id_1: VertexId,
    src_id_2: VertexId,
    src_id_3: VertexId,
    src_id_4: VertexId
  ) extends NxKey {
    override val keys: Seq[VertexId] = Seq(src_id_1, src_id_2, src_id_3, src_id_4)
  }

  final case class N5Key(
    src_id_1: VertexId,
    src_id_2: VertexId,
    src_id_3: VertexId,
    src_id_4: VertexId,
    src_id_5: VertexId
  ) extends NxKey {
    override val keys: Seq[VertexId] = Seq(src_id_1, src_id_2, src_id_3, src_id_4, src_id_5)
  }

}
