package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.Function
import com.kakaopage.crm.extraction.functions.Some

class SomeFactory extends FunctionFactory[extraction.Function, Int, extraction.functions.Some] {
  override def create(function: Some): (Function) => Int = ???
}
