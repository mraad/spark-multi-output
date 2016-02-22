package com.esri

import java.io.{ObjectInputStream, ObjectOutputStream, PrintWriter}
import java.util.UUID

import com.esri.hex.HexGrid
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat

object MainApp extends App with Logging {

  final class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.defaultWriteObject()
      value.write(out)
    }

    private def readObject(in: ObjectInputStream): Unit = {
      value = new Configuration(false)
      value.readFields(in)
    }
  }

  val sc = new SparkContext(new SparkConf())
  try {
    val (inputPath, outputPath, hqlPath) = args.length match {
      case 3 => (args(0), args(1), args(2))
      case _ => throw new IllegalArgumentException("Missing input,output,hql paths")
    }
    val confWrapper = new SerializableConfiguration(sc.hadoopConfiguration)
    val tokens = sc.textFile(inputPath)
      .mapPartitions(iter => {
        val hex25 = new HexGrid(25)
        val hex50 = new HexGrid(50)
        val hex100 = new HexGrid(100)
        val hex200 = new HexGrid(200)
        val fastTok = new FastTok(128)
        val dateTimeParser = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()
        iter.flatMap(line => {
          try {
            val tokens = fastTok.tokenize3(line, ',')
            val pickupDateTime = dateTimeParser.parseDateTime(tokens(5))
            val passengerCount = tokens(7).toInt
            val tripTimeInSec = tokens(8).toInt
            val tripDistance = tokens(9).toDouble
            val pickupLon = tokens(10).toDouble
            val pickupLat = tokens(11).toDouble
            val dropOffLon = tokens(12).toDouble
            val dropOffLat = tokens(13).toDouble
            if (pickupLon == 0.0 || pickupLat == 0.0 || dropOffLon == 0.0 || dropOffLat == 0.0)
              None
            else {
              val pickupX = WebMercator.longitudeToX(pickupLon)
              val pickupY = WebMercator.latitudeToY(pickupLat)
              val dropOffX = WebMercator.longitudeToX(dropOffLon)
              val dropOffY = WebMercator.latitudeToY(dropOffLat)

              val yy = pickupDateTime.getYear
              val mm = pickupDateTime.getMonthOfYear
              val dd = pickupDateTime.getDayOfMonth
              val hh = pickupDateTime.getHourOfDay
              val key = f"$yy%4d/$mm%02d/$dd%02d/$hh%02d"

              val rc25 = hex25.convertXYToRowCol(pickupX, pickupY).toText
              val rc50 = hex50.convertXYToRowCol(pickupX, pickupY).toText
              val rc100 = hex100.convertXYToRowCol(pickupX, pickupY).toText
              val rc200 = hex200.convertXYToRowCol(pickupX, pickupY).toText

              Some(key -> Trip(pickupX, pickupY,
                dropOffX, dropOffY,
                tokens(5), tokens(6),
                passengerCount, tripTimeInSec, tripDistance,
                rc25, rc50, rc100, rc200
              ))
            }
          }
          catch {
            case t: Throwable => {
              log.warn(t.getMessage)
              None
            }
          }
        })
      })
      .groupByKey()
      .mapValues(_.map(_.toText))
      .map {
        case (yymmddhh, iter) => {
          val uuid = UUID.randomUUID().toString
          val name = s"$outputPath/$yymmddhh/$uuid"
          val path = new Path(name)
          val fs = path.getFileSystem(confWrapper.value)
          val outStream = fs.create(path)
          try {
            iter.foreach(text => {
              outStream.writeBytes(text)
              outStream.writeByte('\n')
            })
          } finally {
            outStream.close()
          }
          (yymmddhh, name)
        }
      }
      .collect()

    val pw = new PrintWriter(hqlPath)
    try {
      tokens.foreach {
        case (yymmddhh, name) => {
          val splits = yymmddhh.split('/')
          val yy = splits(0)
          val mm = splits(1)
          val dd = splits(2)
          val hh = splits(3)
          val lastIndex = name.lastIndexOf('/')
          val path = name.substring(0, lastIndex)
          pw.println(s"""alter table trips add if not exists partition (year=$yy,month=$mm,day=$dd,hour=$hh) location '$path';""")
        }
      }
    } finally {
      pw.close()
    }

  }
  finally {
    sc.stop()
  }
}
