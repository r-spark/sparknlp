setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  pos <- nlp_perceptron_pretrained(sc, input_cols = c("sentence", "token"), output_col = "pos")
  chunker <- nlp_chunker(sc, input_cols = c("sentence", "pos"), output_col = "chunk")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "embeddings")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, pos, chunker, embeddings)
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

test_that("chunk_embeddings param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    pooling_strategy = "AVERAGE"
  )

  test_param_setting(sc, nlp_chunk_embeddings, test_args)
})

test_that("nlp_chunk_embeddings spark_connection", {
  test_annotator <- nlp_chunk_embeddings(sc, input_cols = c("chunk", "embeddings"), output_col = "chunk_embeddings")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("chunk_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_chunk_embeddings ml_pipeline", {
  test_annotator <- nlp_chunk_embeddings(pipeline, input_cols = c("chunk", "embeddings"), output_col = "chunk_embeddings")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("chunk_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_chunk_embeddings tbl_spark", {
  transformed_data <- nlp_chunk_embeddings(test_data, input_cols = c("chunk", "embeddings"), output_col = "chunk_embeddings")
  expect_true("chunk_embeddings" %in% colnames(transformed_data))
})

