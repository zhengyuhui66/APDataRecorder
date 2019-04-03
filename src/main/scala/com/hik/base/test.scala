package com.hik.base

object test {
  def main(args: Array[String]): Unit = {
    val regEx="[^0-9]"
    val a="2018-09-10 23:12:21"
    import java.util.regex.Pattern
    val p = Pattern.compile(regEx)
    val m = p.matcher(a)
    println( m.replaceAll("").trim())
  }
}
