package com.nwrs.streaming.parsing

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WordUtilsTest extends FunSuite  {

  test("isCommonWord") {
    assert(WordUtils.isCommonWord("the"))
    assert(!WordUtils.isCommonWord("sdkjfls"))
  }

  test("isCommonWordProfile") {
    assert(WordUtils.isCommonWordProfile("the"))
    assert(WordUtils.isCommonWordProfile("opinions"))
    assert(WordUtils.isCommonWordProfile("twitter"))
    assert(!WordUtils.isCommonWord("sdkjfls"))
  }

  test("isCommonBigramProfile") {
    assert(WordUtils.isCommonBigramProfile("follow back"))
    assert(WordUtils.isCommonBigramProfile("views expressed"))
    assert(!WordUtils.isCommonWord("not common"))
  }

}
