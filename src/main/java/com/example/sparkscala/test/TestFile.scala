package com.example.sparkscala.test

import java.io.{File, PrintWriter}

import scala.io.Source

object TestFile {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("d:\\test1111.txt"))
    writer.write("this is a book")
    writer.close()
    Source.fromFile("d:\\test1111.txt").foreach({
      print
    })
    val me1= me("ddd")
    val me2= me(2)
  }

  object me{
    def apply(x:Int) = 10*x
    def apply(x:String) = x

    def  test()={
      try{
        val f=new File("dd")
      }catch {
       case ex:Exception=>{
          println(ex)
        }
       case _=>{
         println("dd")
       }
      }finally {
        println("kkkkk")
      }
    }
  }

  def matct(x:Any):Any=x match {
    case 1=>"one"
    case _=>"many"
  }

}
