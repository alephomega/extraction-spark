package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction

abstract class FunctionFactory[A, B, C <: extraction.Function] {
  def create(function: C): A => B
}
