package com.example.sparkscala.test

object Test2 {
//  def sum(x:Int,y:Int)=x+y

  def sum(x:Int)(y:Int)=x+y+1

  def main(args: Array[String]): Unit = {
    var count = sum(10)( 20)
    println(count)
  }

}
