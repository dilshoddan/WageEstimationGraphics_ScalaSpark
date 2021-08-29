
import java.io.File
import javax.imageio.ImageIO

import scalafx.application.JFXApp
import scalafx.embed.swing.SwingFXUtils
import scalafx.scene.Scene
import scalafx.collections.ObservableBuffer
import scalafx.scene.chart.LineChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.scene.image.WritableImage
import scala.io.Source

object WageStatChart extends JFXApp {

  stage = new JFXApp.PrimaryStage {

    title = "Sample Calculation Results"
    scene = new Scene {
      root = {
        val xAxis = NumberAxis("Values for X-Axis")
        val yAxis = NumberAxis("Mean Values")

        val toChartData = (xy: (Double, Double)) => XYChart.Data[Number, Number](xy._1, xy._2)

        val data1 = Source.fromFile("data/d1.txt").getLines()
          .map(line => line.split(", "))
          .map(arr => (arr(0).toDouble, arr(1).toDouble))
          .toList

        val series1 = new XYChart.Series[Number, Number] {
          name = "Actual"
          data = data1.map(toChartData)
        }

        val data2 = Source.fromFile("data/d2.txt").getLines()
          .map(line => line.split(", "))
          .map(arr => (arr(0).toDouble, arr(1).toDouble))
          .toList

        val series2 = new XYChart.Series[Number, Number] {
          name = "Estimation"
          data = data2.map(toChartData)
        }

        val error = (data1, data2).zipped.map((d1, d2) =>
          (d1._1, Math.abs(d1._2 - d2._2) * 100 / d1._2)
        )
        println("Absolute Error:")
        error.foreach(println)

        val series3 = new XYChart.Series[Number, Number] {
          name = "Absolute Error"
          data = error.map(toChartData)
        }

        val lineChart = new LineChart[Number, Number](xAxis, yAxis)
        lineChart.getData.add(series1)
        lineChart.getData.add(series2)
//        lineChart.getData.add(series3)
        lineChart.setAnimated(false)

        def savePng: Unit = {
          val img = lineChart.snapshot(null, new WritableImage(500, 250))
          val file = new File("data/chart.png")
          ImageIO.write(SwingFXUtils.fromFXImage(img, null), "png", file)
        }

        lineChart
      }
    }
  }
}

