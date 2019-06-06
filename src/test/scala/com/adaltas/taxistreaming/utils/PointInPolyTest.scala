package com.adaltas.taxistreaming.utils

import org.scalatest.FlatSpec

class PointInPolyTest extends  FlatSpec {

  val manhattanBox: Seq[Seq[Double]] = Vector(
    Seq(-74.0489866963, 40.681530375),
    Seq(-73.8265135518, 40.681530375),
    Seq(-73.8265135518, 40.9548628598),
    Seq(-74.0489866963, 40.9548628598),
    Seq(-74.0489866963, 40.681530375)
  )

  "A Geopoint from Manhattan" must "be inside Manhattan polygon" in {
    val pointManhattan: (Double, Double) = (-73.997940, 40.718320)
    assert(PointInPoly.isPointInPoly(pointManhattan._1, pointManhattan._2, manhattanBox))
  }

  "A Geopoint from Meudon" must "not be inside Manhattan polygon" in {
    val pointMeudon: (Double, Double) = (2.247600, 48.816210)
    assert(!PointInPoly.isPointInPoly(pointMeudon._1, pointMeudon._2, manhattanBox))
  }

  "An arbitrary point (1,1)" must "be inside a square ((0,0),(2,0),(2,2),(0,2))" in {
    assert(PointInPoly.isPointInPoly(1.0, 1.0, Seq(Seq(0,0), Seq(2,0), Seq(2,2), Seq(0,2))))
  }

}
// point_Manhattan
//129 Mulberry St, New York, NY 10013, USA
// Lon, Lat is (-73.997940, 40.718320)