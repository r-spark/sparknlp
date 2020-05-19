setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_classifier_text")
  
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "description", output_col = "document")
  use <- nlp_univ_sent_encoder_pretrained(sc, input_cols = c("document"), output_col = "sentence_embeddings")

  pipeline <- ml_pipeline(assembler, use)
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

test_that("classifier_dl param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    label_col = "string1",
    batch_size = 10,
    max_epochs = 100,
    lr = 0.1,
    dropout = 0.5,
    validation_split = 0.2,
    verbose = 1,
    enable_output_logs = FALSE,
    output_logs_path = "string1",
    lazy_annotator = TRUE
  )

  test_param_setting(sc, nlp_classifier_dl, test_args)
})

test_that("nlp_classifier_dl spark_connection", {
  test_annotator <- nlp_classifier_dl(sc, input_cols = c("sentence_embeddings"), output_col = "class", label_col = "category")
  fit_model <- ml_fit(test_annotator, test_data)
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "class")
})

test_that("nlp_classifier_dl ml_pipeline", {
  test_annotator <- nlp_classifier_dl(pipeline, input_cols = c("sentence_embeddings"), output_col = "class", label_col = "category")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("class" %in% colnames(transformed_data))
})

test_that("nlp_classifier_dl tbl_spark", {
  fit_model <- nlp_classifier_dl(test_data, input_cols = c("sentence_embeddings"), output_col = "class", label_col = "category")
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "class")
})

test_that("nlp_ner_dl pretrained", {
  model <- nlp_classifier_dl_pretrained(sc, input_cols = c("sentence_embeddings"), output_col = "class")
  transformed_data <- ml_transform(model, test_data)
  expect_true("class" %in% colnames(transformed_data))
})

