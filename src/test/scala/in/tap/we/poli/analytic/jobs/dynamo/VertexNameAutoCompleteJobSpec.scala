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
        11L -> "com",
        11L -> "comm",
        11L -> "commi",
        11L -> "commit",
        11L -> "committ",
        11L -> "committe",
        11L -> "committee",
        11 -> "committee1"
      )
    }

    VertexNameAutoComplete.fromVertex(agnosticVertex12) shouldBe {
      Seq(
        1L -> "mic",
        1L -> "mick",
        1L -> "micke",
        1L -> "mickey",
        1L -> "mickey'",
        1L -> "mickey's",
        1L -> "con",
        1L -> "cons",
        1L -> "consu",
        1L -> "consul",
        1L -> "consult",
        1L -> "consulti",
        1L -> "consultin",
        1L -> "consulting",
        1L -> "mickey'sc",
        1L -> "mickey'sco",
        1L -> "mickey'scon",
        1L -> "mickey'scons",
        1L -> "mickey'sconsu",
        1L -> "mickey'sconsul",
        1L -> "mickey'sconsult",
        1L -> "mickey'sconsulti",
        1L -> "mickey'sconsultin",
        1L -> "mickey'sconsulting"
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
        .sortBy(_.prefix) shouldBe {
        Seq(
          VertexNameAutoComplete("com", 3, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("comm", 4, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("commi", 5, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("commit", 6, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("committ", 7, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("committe", 8, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("committee", 9, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("committee1", 10, Set(88, 110, 33, 77, 22, 44, 66, 11, 99, 55)),
          VertexNameAutoComplete("con", 3, Set(1)),
          VertexNameAutoComplete("cons", 4, Set(1)),
          VertexNameAutoComplete("consu", 5, Set(1)),
          VertexNameAutoComplete("consul", 6, Set(1)),
          VertexNameAutoComplete("consult", 7, Set(1)),
          VertexNameAutoComplete("consulti", 8, Set(1)),
          VertexNameAutoComplete("consultin", 9, Set(1)),
          VertexNameAutoComplete("consulting", 10, Set(1)),
          VertexNameAutoComplete("mic", 3, Set(1)),
          VertexNameAutoComplete("mick", 4, Set(1)),
          VertexNameAutoComplete("micke", 5, Set(1)),
          VertexNameAutoComplete("mickey", 6, Set(1)),
          VertexNameAutoComplete("mickey'", 7, Set(1)),
          VertexNameAutoComplete("mickey's", 8, Set(1)),
          VertexNameAutoComplete("mickey'sc", 9, Set(1)),
          VertexNameAutoComplete("mickey'sco", 10, Set(1)),
          VertexNameAutoComplete("mickey'scon", 11, Set(1)),
          VertexNameAutoComplete("mickey'scons", 12, Set(1)),
          VertexNameAutoComplete("mickey'sconsu", 13, Set(1)),
          VertexNameAutoComplete("mickey'sconsul", 14, Set(1)),
          VertexNameAutoComplete("mickey'sconsult", 15, Set(1)),
          VertexNameAutoComplete("mickey'sconsulti", 16, Set(1)),
          VertexNameAutoComplete("mickey'sconsultin", 17, Set(1)),
          VertexNameAutoComplete("mickey'sconsulting", 18, Set(1))
        )
      }
    }
  }

}
