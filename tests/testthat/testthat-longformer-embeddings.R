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

test_that("longformer_embeddings param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    batch_size = 100,
    case_sensitive = FALSE,
    dimension = 768,
    max_sentence_length = 200,
    storage_ref = "string1"
  )

  test_param_setting(sc, nlp_longformer_embeddings, test_args)
})

test_that("nlp_longformer_embeddings spark_connection", {
  test_annotator <- nlp_longformer_embeddings(sc, input_cols = c("document", "token"), output_col = "embeddings")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("embeddings" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_longformer_embeddings"))
})

test_that("nlp_longformer_embeddings ml_pipeline", {
  test_annotator <- nlp_longformer_embeddings(pipeline, input_cols = c("document", "token"), output_col = "embeddings")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("embeddings" %in% colnames(transformed_data))
})

test_that("nlp_longformer_embeddings tbl_spark", {
  transformed_data <- nlp_longformer_embeddings(test_data, input_cols = c("document", "token"), output_col = "embeddings")
  expect_true("embeddings" %in% colnames(transformed_data))
})

