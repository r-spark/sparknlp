setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  assign("sc", sc, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(test_text, envir = .GlobalEnv)
})

test_that("nlp_pretrained_pipeline() tbl_spark", {
  result <- nlp_pretrained_pipeline(text_tbl, "recognize_entities_dl")
  expect_true("entities" %in% colnames(result))
})

test_that("nlp_pretrained_pipeline() spark_connection", {
  result <- nlp_pretrained_pipeline(sc, "recognize_entities_dl")
  expect_equal(jobj_class(result), c("PretrainedPipeline", "Object"))
})

test_that("nlp_pretrained_pipeline annotate", {
  pipeline <- nlp_pretrained_pipeline(sc, "recognize_entities_dl")
  annotations <- nlp_annotate(pipeline, text_tbl, input_col = "text")
  expect_true("entities" %in% colnames(annotations))
})
