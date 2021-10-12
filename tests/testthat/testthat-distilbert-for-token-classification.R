setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  # TODO: put other annotators here as needed

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

test_that("distilbert_for_token_classification param setting", {
# TODO: edit these to make them legal values for the parameters
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    batch_size = 100,
    case_sensitive = FALSE,
    max_sentence_length = 200
  )

  test_param_setting(sc, nlp_distilbert_for_token_classification, test_args)
})

test_that("nlp_distilbert_for_token_classification spark_connection", {
  test_annotator <- nlp_distilbert_for_token_classification(sc, input_cols = c("token", "document"), output_col = "label")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("label" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_distilbert_for_token_classification"))
})

test_that("nlp_distilbert_for_token_classification ml_pipeline", {
  test_annotator <- nlp_distilbert_for_token_classification(pipeline, input_cols = c("token", "document"), output_col = "label")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("label" %in% colnames(transformed_data))
})

test_that("nlp_distilbert_for_token_classification tbl_spark", {
  transformed_data <- nlp_distilbert_for_token_classification(test_data, input_cols = c("token", "document"), output_col = "label")
  expect_true("label" %in% colnames(transformed_data))
})

