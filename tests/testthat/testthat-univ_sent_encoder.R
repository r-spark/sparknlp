setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  # TODO: put other annotators here as needed

  pipeline <- ml_pipeline(assembler, sentdetect)
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

test_that("univ_sent_encoder param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    dimension = 200
  )

  test_param_setting(sc, nlp_univ_sent_encoder, test_args)
})

test_that("nlp_univ_sent_encoder spark_connection", {
  test_annotator <- nlp_univ_sent_encoder(sc, input_cols = c("sentence"), output_col = "use_embeddings")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("use_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_univ_sent_encoder ml_pipeline", {
  test_annotator <- nlp_univ_sent_encoder(pipeline, input_cols = c("sentence"), output_col = "use_embeddings")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("use_embeddings" %in% colnames(transformed_data))
})

test_that("nlp_univ_sent_encoder tbl_spark", {
  transformed_data <- nlp_univ_sent_encoder(test_data, input_cols = "sentence", output_col = "use_embeddings")
  expect_true("use_embeddings" %in% colnames(transformed_data))
})

