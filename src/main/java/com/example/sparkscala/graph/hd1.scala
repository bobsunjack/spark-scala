package com.example.sparkscala.graph

object hd1 {
  def main(args: Array[String]): Unit = {
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
    val conf = new org.apache.hadoop.conf.Configuration
    conf.set("fs.defaultFS", "hdfs://localhost")
    val fs = FileSystem.get(conf)
    FileUtil.copyMerge(fs, new Path("/user/cloudera/myGraphVertices/"),
      fs, new Path("/user/cloudera/myGraphVerticesFile"), false, conf, null)
  }
}
