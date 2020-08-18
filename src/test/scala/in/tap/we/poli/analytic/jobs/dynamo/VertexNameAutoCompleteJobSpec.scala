package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.VertexNameAutoCompleteJob.VertexNameAutoComplete
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
    val _: Unit = {
      val resourcePath: String = {
        "/Users/alex/Documents/GitHub/Alex/tap-in/we-poli/we-poli-analytic/src/test/resources/dynamo/vertex_name_auto_complete"
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
      agnosticVertices.toDS().write.mode(SaveMode.Overwrite).json(in1Path)
      aggregateExpenditureEdges.toDS.write.mode(SaveMode.Overwrite).parquet(in2Path)
      new VertexNameAutoCompleteJob(
        TwoInArgs(In(in1Path), In(in2Path, Formats.PARQUET)),
        OneOutArgs(Out(outPath)),
        MAX_RESPONSE_SIZE = 10
      ).execute()
      spark
        .read
        .json(outPath)
        .as[VertexNameAutoCompleteJob.VertexNameAutoComplete]
        .collect()
        .toSeq
        .sortBy(_.prefix)
        .map { autoComplete: VertexNameAutoComplete =>
          (autoComplete.prefix, autoComplete.prefix_size, autoComplete.vertices.map(_.uid))
        } shouldBe {
        Seq(
          ("com", 3, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("comm", 4, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("commi", 5, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("commit", 6, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("committ", 7, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("committe", 8, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("committee", 9, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("committee1", 10, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          ("con", 3, Set(1)),
          ("cons", 4, Set(1)),
          ("consu", 5, Set(1)),
          ("consul", 6, Set(1)),
          ("consult", 7, Set(1)),
          ("consulti", 8, Set(1)),
          ("consultin", 9, Set(1)),
          ("consulting", 10, Set(1)),
          ("mic", 3, Set(1)),
          ("mick", 4, Set(1)),
          ("micke", 5, Set(1)),
          ("mickey", 6, Set(1)),
          ("mickey'", 7, Set(1)),
          ("mickey's", 8, Set(1)),
          ("mickey'sc", 9, Set(1)),
          ("mickey'sco", 10, Set(1)),
          ("mickey'scon", 11, Set(1)),
          ("mickey'scons", 12, Set(1)),
          ("mickey'sconsu", 13, Set(1)),
          ("mickey'sconsul", 14, Set(1)),
          ("mickey'sconsult", 15, Set(1)),
          ("mickey'sconsulti", 16, Set(1)),
          ("mickey'sconsultin", 17, Set(1)),
          ("mickey'sconsulting", 18, Set(1))
        )
      }
    }
  }

}
