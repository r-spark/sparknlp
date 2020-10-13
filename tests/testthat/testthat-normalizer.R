setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("normalizer param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    cleanup_patterns = c("string1", "string2"),
    lowercase = FALSE
  )

  test_param_setting(sc, nlp_normalizer, test_args)
})

test_that("nlp_normalizer spark_connection", {
  test_annotator <- nlp_normalizer(sc, input_cols = c("token"), output_col = "normalized")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("normalized" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_normalizer"))
  expect_true(inherits(fit_model, "nlp_normalizer_model"))
})

test_that("nlp_normalizer ml_pipeline", {
  test_annotator <- nlp_normalizer(pipeline, input_cols = c("token"), output_col = "normalized")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("normalized" %in% colnames(transformed_data))
})

test_that("nlp_normalizer tbl_spark", {
  transformed_data <- nlp_normalizer(test_data, input_cols = c("token"), output_col = "normalized")
  expect_true("normalized" %in% colnames(transformed_data))
})

