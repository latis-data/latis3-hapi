package latis.ops

import latis.data.SampledFunction

trait FunctionalAlgebra {
  def select(vname: String, operator: String, value: String): SampledFunction
}

object FunctionalAlgebra {
  
  object ImplicitOps {
    implicit class FunctionOps(f: SampledFunction) extends FunctionalAlgebra {
      def select(vname: String, operator: String, value: String): SampledFunction = ???
    }
  }

}