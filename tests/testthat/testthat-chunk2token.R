setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  ngram <- nlp_ngram_generator(sc, input_cols = c("token"), output_col = "ngram", n = 2)


  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, ngram)
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

test_that("chunk2token param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1"
  )

  test_param_setting(sc, nlp_chunk2token, test_args)
})

test_that("nlp_chunk2token spark_connection", {
  test_annotator <- nlp_chunk2token(sc, input_cols = c("ngram"), output_col = "token_chunk")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("token_chunk" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_chunk2token"))
})

test_that("nlp_chunk2token ml_pipeline", {
  test_annotator <- nlp_chunk2token(pipeline, input_cols = c("ngram"), output_col = "token_chunk")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("token_chunk" %in% colnames(transformed_data))
})

test_that("nlp_chunk2token tbl_spark", {
  transformed_data <- nlp_chunk2token(test_data, input_cols = c("ngram"), output_col = "token_chunk")
  expect_true("token_chunk" %in% colnames(transformed_data))
})

