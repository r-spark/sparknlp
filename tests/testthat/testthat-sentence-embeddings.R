setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "embeddings")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings)
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

test_that("sentence_embeddings param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    pooling_strategy = "AVERAGE"
  )

  test_param_setting(sc, nlp_sentence_embeddings, test_args)
})

test_that("nlp_sentence_embeddings spark_connection", {
  test_annotator <- nlp_sentence_embeddings(sc, input_cols = c("document", "embeddings"), output_col = "sentence_embeddings")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("sentence_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_sentence_embeddings ml_pipeline", {
  test_annotator <- nlp_sentence_embeddings(pipeline, input_cols = c("document", "embeddings"), output_col = "sentence_embeddings")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("sentence_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_sentence_embeddings tbl_spark", {
  transformed_data <- nlp_sentence_embeddings(test_data, input_cols = c("document", "embeddings"), output_col = "sentence_embeddings")
  expect_true("sentence_embeddings" %in% colnames(transformed_data))
})

