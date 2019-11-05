setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "embeddings")

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

test_that("embeddings_finisher param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_cols = c("string1"),
    clean_annotations = TRUE,
    output_as_vector = TRUE
  )

  test_param_setting(sc, nlp_embeddings_finisher, test_args)
})

test_that("nlp_embeddings_finisher spark_connection", {
  test_annotator <- nlp_embeddings_finisher(sc, input_cols = c("embeddings"), output_cols = c("embeddings_vectors"))
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("embeddings_vectors" %in% colnames(transformed_data))
})

test_that("nlp_embeddings_finisher ml_pipeline", {
  test_annotator <- nlp_embeddings_finisher(pipeline, input_cols = c("embeddings"), output_cols = c("embeddings_vectors"))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("embeddings_vectors" %in% colnames(transformed_data))
})

test_that("nlp_embeddings_finisher tbl_spark", {
  transformed_data <- nlp_embeddings_finisher(test_data, input_cols = c("embeddings"), output_cols = c("embeddings_vectors"))
  expect_true("embeddings_vectors" %in% colnames(transformed_data))
})

