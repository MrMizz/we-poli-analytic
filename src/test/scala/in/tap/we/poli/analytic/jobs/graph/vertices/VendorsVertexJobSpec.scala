package in.tap.we.poli.analytic.jobs.graph.vertices

import org.scalatest.{FlatSpec, Matchers}

class VendorsVertexJobSpec extends FlatSpec with Matchers with VendorsVertexJobFixtures {

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
