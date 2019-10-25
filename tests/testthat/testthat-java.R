setup({
  sc <- testthat_spark_connection()
  assign("sc", sc, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
})

test_that("default arguments static method", {
  expect_equal(default_argument_static(sc, "com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel", "pretrained", 1), "ner_crf")
})

test_that("default arguments constructor", {
  expect_equal(default_argument_static(sc, "com.johnsnowlabs.nlp.pretrained.PretrainedPipeline", "constructor", 2), "en")
})

test_that("default arguments instance method", {
  model <- invoke_new(sc, "com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfApproach")
  value <- default_argument(model, "setExternalFeatures", 3)
  expect_equal(invoke(value, "toString"), "LINE_BY_LINE")
})