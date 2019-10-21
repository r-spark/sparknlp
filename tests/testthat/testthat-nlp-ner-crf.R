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

test_that("nlp_ner_crf param setting", {
# TODO: edit these to make them legal values for the parameters
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    c("label_col", "min_epochs", "max_epochs", "l2", "C0", "loss_eps", "min_w", "external_features", "entities", "verbose", "random_seed" = "string1")
  )
  test_param_setting(sc, nlp_nlp_ner_crf, test_args)
})

test_that("nlp_ner_crf spark_connection", {
  test_annotator <- nlp_ner_crf(sc, input_cols = sentence, output_col = "ner")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_ner_crf spark_connection", {
  test_annotator <- nlp_ner_crf(sc, input_cols = token, output_col = "ner")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_nlp_ner_crf spark_connection", {
  test_annotator <- nlp_nlp_ner_crf(sc, input_cols = pos, output_col = "ner")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_nlp_ner_crf ml_pipeline", {
  test_annotator <- nlp_ner_crf(pipeline, input_cols = sentence, output_col = "ner")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_nlp_ner_crf ml_pipeline", {
  test_annotator <- nlp_ner_crf(pipeline, input_cols = token, output_col = "ner")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_ner_crf ml_pipeline", {
  test_annotator <- nlp_ner_crf(pipeline, input_cols = pos, output_col = "ner")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_ner_crf tbl_spark", {
  transformed_data <- nlp_ner_crf(test_data, input_cols = sentence, output_col = "ner")
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_ner_crf tbl_spark", {
  transformed_data <- nlp_ner_crf(test_data, input_cols = token, output_col = "ner")
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_nlp_ner_crf tbl_spark", {
  transformed_data <- nlp_ner_crf(test_data, input_cols = pos, output_col = "ner")
  expect_true("ner" %in% colnames(transformed_data))
})

