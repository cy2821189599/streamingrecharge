package demo

import java.text.DecimalFormat

object testDemo {
  def main(args: Array[String]): Unit = {
    val format = new DecimalFormat("#0.0")
    println(format.format(1.233423))
  }

}
