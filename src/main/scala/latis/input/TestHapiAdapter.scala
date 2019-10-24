package latis.input

import latis.model.Dataset
import latis.output.TextWriter
import latis.ops.FunctionalAlgebra.ImplicitOps._
import latis.data.HapiFunction

object TestHapiAdapter extends App {
  
    val ds = Dataset.fromName("sorce_tsi")
    
    val d0: HapiFunction = ds.data.asInstanceOf[HapiFunction]
    val d1: HapiFunction = d0.select("time", ">", "2010-01-01").asInstanceOf[HapiFunction]
    val d2 = d1.select("time", "<", "2011-01-01")
    val ds2 = ds.copy(data=d2)
    
    TextWriter(System.out).write(ds2)
  
}