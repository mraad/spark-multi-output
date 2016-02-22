package com.esri

/**
  */
case class Trip(val pickupX: Double,
                val pickupY: Double,
                val dropOffX: Double,
                val dropOffY: Double,
                val pickupDateTime: String,
                val dropOffDateTime: String,
                val passengerCount: Int,
                val tripTimeInSec: Int,
                val tripDistance: Double,
                val rc25: String,
                val rc50: String,
                val rc100: String,
                val rc200: String
               ) {
  def toText() = f"$pickupDateTime,$dropOffDateTime,$pickupX%.1f,$pickupY%.1f,$dropOffX%.1f,$dropOffY%.1f,$passengerCount,$tripTimeInSec,$tripDistance,$rc25,$rc50,$rc100,$rc200"
}
