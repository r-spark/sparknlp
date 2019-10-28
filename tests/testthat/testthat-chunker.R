setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  pos <- nlp_perceptron_pretrained(sc, input_cols = c("sentence", "token"), output_col = "pos")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, pos)
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

test_that("nlp_chunker param setting", {
  test_args <- list(
    input_cols = c("sentence", "pos"),
    output_col = "chunk",
    regex_parsers = c("<DT>?<JJ>*<NN>", "<NNP>+")
  )

  test_param_setting(sc, nlp_chunker, test_args)
})

test_that("nlp_nlp_chunker spark_connection", {
  test_annotator <- nlp_chunker(sc, input_cols = c("sentence","pos"), output_col = "chunk")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("chunk" %in% colnames(transformed_data))
})

test_that("nlp_nlp_chunker ml_pipeline", {
  test_annotator <- nlp_chunker(pipeline, input_cols = c("sentence","pos"), output_col = "chunk")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("chunk" %in% colnames(transformed_data))
})

test_that("nlp_nlp_chunker tbl_spark", {
  transformed_data <- nlp_chunker(test_data, input_cols = c("sentence","pos"), output_col = "chunk")
  expect_true("chunk" %in% colnames(transformed_data))
})

