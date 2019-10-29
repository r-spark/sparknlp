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

test_that("nlp_ngram_generator spark_connection", {
  test_annotator <- nlp_ngram_generator(sc, input_cols = c("token"), output_col = "ngrams", n = 2)
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("ngrams" %in% colnames(transformed_data))
})

test_that("nlp_ngram_generator ml_pipeline", {
  test_annotator <- nlp_ngram_generator(pipeline, input_cols = c("token"), output_col = "ngrams", n = 2, enable_cumulative = TRUE)
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("ngrams" %in% colnames(transformed_data))
})

test_that("nlp_ngram_generator tbl_spark", {
  transformed_data <- nlp_ngram_generator(test_data, input_cols = c("token"), output_col = "ngrams")
  expect_true("ngrams" %in% colnames(transformed_data))
})

