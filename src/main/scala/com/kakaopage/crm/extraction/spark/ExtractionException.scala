package com.kakaopage.crm.extraction.spark

class ExtractionException(message: String, cause: Throwable) extends RuntimeException(message) {
  if (cause != null) initCause(cause)

  def this(message: String) = this(message, null)
}
