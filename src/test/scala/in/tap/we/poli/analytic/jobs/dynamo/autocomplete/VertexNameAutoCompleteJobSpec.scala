package in.tap.we.poli.analytic.jobs.dynamo.autocomplete

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.VertexNameAutoCompleteJob.VertexNameAutoComplete
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SaveMode

class VertexNameAutoCompleteJobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "take top sorted" in {
    val actual = {
      Set(
        (agnosticVertex1, 7L),
        (agnosticVertex1, 2L),
        (agnosticVertex1, 3L),
        (agnosticVertex1, 5L),
        (agnosticVertex1, 1L),
        (agnosticVertex1, 4L),
        (agnosticVertex1, 6L)
      )
    }
    VertexNameAutoComplete.takeTop(3)(actual) shouldBe {
      Seq(
        (agnosticVertex1, 7L),
        (agnosticVertex1, 6L),
        (agnosticVertex1, 5L)
      )
    }
  }

  it should "build prefixes from vertex names" in {
    VertexNameAutoComplete.fromVertex(agnosticVertex1).sortBy(_._2._2) shouldBe {
      Seq(
        11L -> (agnosticVertex1 -> "com"),
        11L -> (agnosticVertex1 -> "comm"),
        11L -> (agnosticVertex1 -> "commi"),
        11L -> (agnosticVertex1 -> "commit"),
        11L -> (agnosticVertex1 -> "committ"),
        11L -> (agnosticVertex1 -> "committe"),
        11L -> (agnosticVertex1 -> "committee"),
        11L -> (agnosticVertex1 -> "committee1")
      )
    }

    VertexNameAutoComplete.fromVertex(agnosticVertex12).sortBy(_._2._2) shouldBe {
      Seq(
        1L -> (agnosticVertex12 -> "con"),
        1L -> (agnosticVertex12 -> "cons"),
        1L -> (agnosticVertex12 -> "consu"),
        1L -> (agnosticVertex12 -> "consul"),
        1L -> (agnosticVertex12 -> "consult"),
        1L -> (agnosticVertex12 -> "consulti"),
        1L -> (agnosticVertex12 -> "consultin"),
        1L -> (agnosticVertex12 -> "consulting"),
        1L -> (agnosticVertex12 -> "mcd"),
        1L -> (agnosticVertex12 -> "mcdo"),
        1L -> (agnosticVertex12 -> "mcdon"),
        1L -> (agnosticVertex12 -> "mcdona"),
        1L -> (agnosticVertex12 -> "mcdonal"),
        1L -> (agnosticVertex12 -> "mcdonald"),
        1L -> (agnosticVertex12 -> "mcdonald'"),
        1L -> (agnosticVertex12 -> "mcdonald's"),
        1L -> (agnosticVertex12 -> "mic"),
        1L -> (agnosticVertex12 -> "mick"),
        1L -> (agnosticVertex12 -> "micke"),
        1L -> (agnosticVertex12 -> "mickey"),
        1L -> (agnosticVertex12 -> "mickey'"),
        1L -> (agnosticVertex12 -> "mickey's"),
        1L -> (agnosticVertex12 -> "mickey'sc"),
        1L -> (agnosticVertex12 -> "mickey'sco"),
        1L -> (agnosticVertex12 -> "mickey'scon"),
        1L -> (agnosticVertex12 -> "mickey'scons"),
        1L -> (agnosticVertex12 -> "mickey'sconsu"),
        1L -> (agnosticVertex12 -> "mickey'sconsul"),
        1L -> (agnosticVertex12 -> "mickey'sconsult"),
        1L -> (agnosticVertex12 -> "mickey'sconsulti"),
        1L -> (agnosticVertex12 -> "mickey'sconsultin"),
        1L -> (agnosticVertex12 -> "mickey'sconsulting")
      )
    }
  }

  it should "build top N prefix-bound uid collections" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../dynamo/vertex_name_auto_complete/").toString
    }
    val in1Path: String = {
      s"$resourcePath/in1/"
    }
    val in2Path: String = {
      s"$resourcePath/in2/"
    }
    val outPath: String = {
      s"$resourcePath/out/"
    }
    import spark.implicits._
    agnosticVertices.toDS().write.mode(SaveMode.Overwrite).parquet(in1Path)
    aggregateExpenditureEdges.toDS.write.mode(SaveMode.Overwrite).parquet(in2Path)
    new VertexNameAutoCompleteJob(
      TwoInArgs(In(in1Path, Formats.PARQUET), In(in2Path, Formats.PARQUET)),
      OneOutArgs(Out(outPath, Formats.PARQUET)),
      MAX_RESPONSE_SIZE = 10
    ).execute()
    spark
      .read
      .parquet(outPath)
      .as[VertexNameAutoCompleteJob.VertexNameAutoComplete]
      .collect()
      .toSeq
      .sortBy(_.prefix)
      .map { autoComplete: VertexNameAutoComplete =>
        val pretty: (String, Long, Seq[VertexId]) = {
          (autoComplete.prefix, autoComplete.prefix_size, autoComplete.vertexIds)
        }
        val prettyPrint = {
          "(" ++ "\"" ++ pretty._1 ++ "\"" ++ ", " ++ pretty._2.toString ++ ", " ++ pretty._3.toString ++ ")"
        }
        println(prettyPrint)
        pretty
      } shouldBe {
      Seq(
        ("com_false", 3, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("com_true", 3, List(11)),
        ("comm_false", 4, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("comm_true", 4, List(11)),
        ("commi_false", 5, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("commi_true", 5, List(11)),
        ("commit_false", 6, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("commit_true", 6, List(11)),
        ("committ_false", 7, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("committ_true", 7, List(11)),
        ("committe_false", 8, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("committe_true", 8, List(11)),
        ("committee1_false", 10, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("committee1_true", 10, List(11)),
        ("committee_false", 9, List(121, 110, 99, 55, 44, 33, 22, 88, 77, 66)),
        ("committee_true", 9, List(11)),
        ("con_false", 3, List(1)),
        ("cons_false", 4, List(1)),
        ("consu_false", 5, List(1)),
        ("consul_false", 6, List(1)),
        ("consult_false", 7, List(1)),
        ("consulti_false", 8, List(1)),
        ("consultin_false", 9, List(1)),
        ("consulting_false", 10, List(1)),
        ("mcd_false", 3, List(1)),
        ("mcdo_false", 4, List(1)),
        ("mcdon_false", 5, List(1)),
        ("mcdona_false", 6, List(1)),
        ("mcdonal_false", 7, List(1)),
        ("mcdonald'_false", 9, List(1)),
        ("mcdonald's_false", 10, List(1)),
        ("mcdonald_false", 8, List(1)),
        ("mic_false", 3, List(1)),
        ("mick_false", 4, List(1)),
        ("micke_false", 5, List(1)),
        ("mickey'_false", 7, List(1)),
        ("mickey's_false", 8, List(1)),
        ("mickey'sc_false", 9, List(1)),
        ("mickey'sco_false", 10, List(1)),
        ("mickey'scon_false", 11, List(1)),
        ("mickey'scons_false", 12, List(1)),
        ("mickey'sconsu_false", 13, List(1)),
        ("mickey'sconsul_false", 14, List(1)),
        ("mickey'sconsult_false", 15, List(1)),
        ("mickey'sconsulti_false", 16, List(1)),
        ("mickey'sconsultin_false", 17, List(1)),
        ("mickey'sconsulting_false", 18, List(1)),
        ("mickey_false", 6, List(1))
      )
    }
  }

}
