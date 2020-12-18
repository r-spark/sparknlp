setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "normalized")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
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

test_that("token_assembler param setting", {
# TODO: edit these to make them legal values for the parameters
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1"
  )

  test_param_setting(sc, nlp_token_assembler, test_args)
})

test_that("nlp_token_assembler spark_connection", {
  test_annotator <- nlp_token_assembler(sc, input_cols = c("document", "normalized"), output_col = "assembled")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("assembled" %in% colnames(transformed_data))
})

test_that("nlp_token_assembler ml_pipeline", {
  test_annotator <- nlp_token_assembler(pipeline, input_cols = c("document", "normalized"), output_col = "assembled")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("assembled" %in% colnames(transformed_data))
})

test_that("nlp_token_assembler tbl_spark", {
  transformed_data <- nlp_token_assembler(test_data, input_cols = c("document", "normalized"), output_col = "assembled")
  expect_true("assembled" %in% colnames(transformed_data))
})

