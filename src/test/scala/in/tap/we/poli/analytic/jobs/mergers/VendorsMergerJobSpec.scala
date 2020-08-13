package in.tap.we.poli.analytic.jobs.mergers

import in.tap.base.spark.io.{In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.connectors.VendorsConnectorJobFixtures
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor._

class VendorsMergerJobSpec extends BaseSparkJobSpec with VendorsMergerJobFixtures {

  it should "reduce two vendors found to be the same entity" in {
    reduce(fromVendor(vendor1), fromVendor(vendor2)) shouldBe {
      UniqueVendor(
        uid = 1L,
        uids = Seq(1L, 2L),
        name = "Vendor1",
        names = Set("Vendor1", "Vendor2"),
        city = Some("City1"),
        state = Some("State1"),
        zip_code = Some("Zip1"),
        num_merged = 2,
        memos = Set("memo1"),
        edges = Set(edge1, edge2)
      )
    }
  }

  it should "merge vendors from connector" in {
    val resourcePath: String = {
      "/Users/alex/Documents/GitHub/Alex/tap-in/we-poli/we-poli-analytic/src/test/resources/mergers/vendors/"
    }

    val in1Path: String = {
      s"$resourcePath/in1"
    }

    val in2Path: String = {
      s"$resourcePath/in2"
    }

    val outPath: String = {
      s"$resourcePath/mergers/vendors/out"
    }

    new VendorsMergerJob(
      TwoInArgs(In(path = in1Path), In(path = in2Path)),
      OneOutArgs(Out(path = outPath))
    ).execute()

    object ConnectorMockEdges extends VendorsConnectorJobFixtures

    import spark.implicits._
    spark.read.json(outPath).as[UniqueVendor].collect.toSeq.sortBy(_.uid) shouldBe {
      Seq(
        UniqueVendor(
          uid = 1L,
          uids = Seq(3L, 1L),
          name = "Vendor",
          names = Set("Vendor", "Vendor, Inc. # 1"),
          city = Some("City1"),
          state = Some("State1"),
          zip_code = Some("Zip1"),
          num_merged = 2,
          memos = Set("memo1"),
          edges = Set(ConnectorMockEdges.edge3, ConnectorMockEdges.edge1)
        ),
        UniqueVendor(
          uid = 2L,
          uids = Seq(2L),
          name = "Vendor Two",
          names = Set("Vendor Two"),
          city = Some("City1"),
          state = Some("State1"),
          zip_code = Some("Zip1"),
          num_merged = 1,
          memos = Set(),
          edges = Set(ConnectorMockEdges.edge2)
        )
      )
    }
  }

}
