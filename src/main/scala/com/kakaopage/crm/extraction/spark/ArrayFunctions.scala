package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ArrayFunctions {

  def element = udf((a: Seq[_], at: Int) => {
    a(at)
  })

  val invoke: (extraction.Function) => Column = {

    case f: Cardinality => {
      size(invoke(f.getArray))
    }

    case f: ElementAt => {
      element(invoke(f.getArray), lit(f.getIndex))
    }

//    case f: Filter => {
//
//    }
//
//    case f: MaxOf => {
//
//    }
//
//    case f: MinOf => {
//
//    }
//
//    case f: SumOf => {
//
//    }
//
//    case f: ArrayOf => {
//
//    }
  }
}
