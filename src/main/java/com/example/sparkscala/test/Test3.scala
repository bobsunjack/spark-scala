package com.example.sparkscala.test
import java.io.FileReader

import Array._
object Test3 {
  def bob(k1: (Int,Int) => Int): Int = {
    k1(1,2)
    return 2
  }
  def main(args: Array[String]): Unit = {
    var tt: String = "test"
    var a = "kkk"
    var a1, b = 10
    val t1 =(x:Int,y:Int) => {
      println("this")
      println("other")
      return 1
    }
    val t2=() => 1



    val m = bob _
    val p=m(t1(1,2))
    val kk = (a: Int, b: Int) => {
      return 1;
    }
    def me(a:Int,b:Int):String={
      return "this book"
    }

    var pp=new StringBuilder
    pp++="aaa"
    pp+='3'
    println(pp.toString)

    var z:Array[String]=new Array[String](2)
    z(0)="ddd"
    var p1= Array("a", "b")
    var myMatrix=ofDim[Int](3,3)
    for(i <- 0 to 2) {
      for(j <- 0 to 2) {
         myMatrix(i)(j)=j
      }
    }

    val x= List(1, 2)
    val x1= Set(1, 2)
    val x2= Map(1 -> 1, 2 -> 2)
    val x3 = (10, 2)
    val x4= Some(2)

    trait  Show{
      def test(me:Int):Unit= {
        print("this is book")
      }
    }

    def matchTest(x:Int):String=x match {
      case 1=>"one"
    }

    class  User(name1:String){
      var name=name1
      def me(name:String):String={
       return "kkkk"
      }
    }
    class User2(name1:String) extends User(name1){

    }

    val use1 = new User("kkk")
    use1.me("ipp")

    class Marker private (str:String){

    }

    object  Marker{

    }

    try{
      val f = new FileReader("input.txt")
    }catch {
      case ex:Exception=>{
      }
    }


     case class My(name:String,age:Int){

     }
    val z= My("tt",1);


  }
}
