setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_data(data.frame(text = "The cat ate the mouse"), "sentdetectdata")
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  document_data <<- ml_transform(assembler, text_tbl)
  
  assign("sc", sc, envir = parent.frame())
  assign("document_data", document_data, envir = parent.frame())
})

teardown({
  sparklyr::tbl_uncache(sc, "sentdetectdata")
  rm(sc, envir = .GlobalEnv)
  rm(document_data, envir = .GlobalEnv)
})

test_that("nlp_sentence_detector() param setting", {
  test_args <- list(
    input_cols = c("document"), 
    output_col = "sentence",
    custom_bounds = c(":"),
    use_custom_only = FALSE,
    use_abbreviations = TRUE,
    explode_sentences = FALSE)
  test_param_setting(sc, nlp_sentence_detector, test_args)
})

test_that("nlp_sentence_detector() spark_connection", {
  detector <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  transformed_data <- ml_transform(detector, document_data)

  expect_true("sentence" %in% colnames(transformed_data))
})


test_that("nlp_sentence_detector() ml_pipeline", {
  detector <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  pipeline <- ml_pipeline(detector)

  transformed_data <- ml_fit_and_transform(pipeline, document_data)

  expect_true("sentence" %in% colnames(transformed_data))
})

test_that("nlp_sentence_detector() tbl_spark", {
  transformed_data <- nlp_sentence_detector(document_data, input_cols = c("document"), output_col = "sentence")
  expect_true("document" %in% colnames(transformed_data))
})
