package edu.fuberlin

import com.esri.core.geometry.Point
import org.joda.time.DateTime

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  case class Trip(vendorID: Int,
             pickupTime: DateTime,
             dropoffTime: DateTime,
             passengerCount: Int,
             tripDistance: Double,
             pickupLocation: Point,
             rateCodeID: Int,
             storeAndForwardFlag: String,
             dropoffLocation: Point,
             paymentType: Int,
             fareAmount: Double,
             extra: Double,
             mtaTax: Double,
             tipAmount: Double,
             tollsAmount: Double,
             improvementSurcharge: Double,
             totalAmount: Double
             )

  def parse(line: String): Trip = {
    Trip(2, new DateTime(), new DateTime(), 0, 0, new Point(0,0), 0, "",
      new Point(0,0), 0, 0, 0, 0, 0, 0, 0, 0)

  }
}