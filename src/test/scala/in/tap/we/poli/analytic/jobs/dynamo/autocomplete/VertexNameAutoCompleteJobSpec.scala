package in.tap.we.poli.analytic.jobs.dynamo.autocomplete

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.VertexNameAutoCompleteJob.VertexNameAutoComplete
import org.apache.spark.sql.SaveMode

class VertexNameAutoCompleteJobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "build prefixes from vertex names" in {
    VertexNameAutoComplete.fromVertex(agnosticVertex1) shouldBe {
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

    VertexNameAutoComplete.fromVertex(agnosticVertex12) shouldBe {
      Seq(
        1L -> (agnosticVertex12 -> "mic"),
        1L -> (agnosticVertex12 -> "mick"),
        1L -> (agnosticVertex12 -> "micke"),
        1L -> (agnosticVertex12 -> "mickey"),
        1L -> (agnosticVertex12 -> "mickey'"),
        1L -> (agnosticVertex12 -> "mickey's"),
        1L -> (agnosticVertex12 -> "con"),
        1L -> (agnosticVertex12 -> "cons"),
        1L -> (agnosticVertex12 -> "consu"),
        1L -> (agnosticVertex12 -> "consul"),
        1L -> (agnosticVertex12 -> "consult"),
        1L -> (agnosticVertex12 -> "consulti"),
        1L -> (agnosticVertex12 -> "consultin"),
        1L -> (agnosticVertex12 -> "consulting"),
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
      .sortBy(_.prefix) // TODO: should not sort
      .map { autoComplete: VertexNameAutoComplete =>
        (autoComplete.prefix, autoComplete.prefix_size, autoComplete.vertices.map(_.uid))
      } shouldBe {
      Seq(
        ("com_true", 3, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("comm_true", 4, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("commi_true", 5, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("commit_true", 6, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("committ_true", 7, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("committe_true", 8, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("committee1_true", 10, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("committee_true", 9, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
        ("con_false", 3, Set(1)),
        ("cons_false", 4, Set(1)),
        ("consu_false", 5, Set(1)),
        ("consul_false", 6, Set(1)),
        ("consult_false", 7, Set(1)),
        ("consulti_false", 8, Set(1)),
        ("consultin_false", 9, Set(1)),
        ("consulting_false", 10, Set(1)),
        ("mic_false", 3, Set(1)),
        ("mick_false", 4, Set(1)),
        ("micke_false", 5, Set(1)),
        ("mickey'_false", 7, Set(1)),
        ("mickey's_false", 8, Set(1)),
        ("mickey'sc_false", 9, Set(1)),
        ("mickey'sco_false", 10, Set(1)),
        ("mickey'scon_false", 11, Set(1)),
        ("mickey'scons_false", 12, Set(1)),
        ("mickey'sconsu_false", 13, Set(1)),
        ("mickey'sconsul_false", 14, Set(1)),
        ("mickey'sconsult_false", 15, Set(1)),
        ("mickey'sconsulti_false", 16, Set(1)),
        ("mickey'sconsultin_false", 17, Set(1)),
        ("mickey'sconsulting_false", 18, Set(1)),
        ("mickey_false", 6, Set(1))
      )
    }
  }

}
