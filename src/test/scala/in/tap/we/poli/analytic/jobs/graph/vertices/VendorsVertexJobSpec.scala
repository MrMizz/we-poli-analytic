package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.BaseSpec

class VendorsVertexJobSpec extends BaseSpec with VendorsVertexJobFixtures {

  it should "attribute a vendor into a vertex" in {
    import VendorsVertexJob._

    VendorVertex.fromVendor(uniqueVendor1) shouldBe {
      vendorVertex1
    }

    VendorVertex.fromVendor(uniqueVendor2) shouldBe {
      vendorVertex2
    }

    VendorVertex.fromVendor(uniqueVendor3) shouldBe {
      vendorVertex3
    }
  }

}
