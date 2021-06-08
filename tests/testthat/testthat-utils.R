setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")

  pipeline <- ml_pipeline(assembler)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("nlp_set_param", {
  model <- nlp_language_detector_dl_pretrained(sc, input_cols = c("document"), output_col = "language")
  oldvalue <- ml_param(model, "coalesce_sentences")
  newmodel <- nlp_set_param(model, "coalesce_sentences", !oldvalue)
  newvalue <- ml_param(newmodel, "coalesce_sentences")
  
  expect_equal(newvalue, !oldvalue)
  
  # Test setting a Float parameter
  oldvalue <- ml_param(model, "threshold")
  newmodel <- nlp_set_param(model, "threshold", 0.8)
  newvalue <- ml_param(newmodel, "threshold")
  
  expect_equal(newvalue, 0.8)
})

test_that("nlp_conll_read_dataset", {
  conll_data <- nlp_conll_read_dataset(sc, here::here("tests", "testthat", "data", "eng.testa.conll"))
  expect_true("text" %in% colnames(conll_data))
})

test_that("nlp_conllu_read_dataset", {
  conllu_data <- nlp_conllu_read_dataset(sc, here::here("tests", "testthat", "data", "en.test.conllu"))
  expect_true("text" %in% colnames(conllu_data))
})

