setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_classifier_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "description", output_col = "document")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  normalizer <- nlp_normalizer(sc, input_cols = c("token"), output_col = "normalized")
  stopwords_cleaner <- nlp_stop_words_cleaner(sc, input_cols = c("normalized"), output_col = "clean_tokens")
  stemmer <- nlp_stemmer(sc, input_cols = c("clean_tokens"), output_col = "stem")

  pipeline <- ml_pipeline(assembler, tokenizer, normalizer, stopwords_cleaner, stemmer)
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

test_that("document_logreg_classifier param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string2",
    fit_intercept = TRUE,
    #label_column = "string3",
    labels = c("string4", "string5"),
    max_iter = 10,
    merge_chunks = TRUE,
    tol = 0.001
  )

  test_param_setting(sc, nlp_document_logreg_classifier, test_args)
})

test_that("nlp_document_logreg_classifier spark_connection", {
  test_annotator <- nlp_document_logreg_classifier(sc, input_cols = c("stem"), output_col = "document_class",
                                                   label_column = "category")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("document_class" %in% colnames(transformed_data))

  expect_true(inherits(test_annotator, "nlp_document_logreg_classifier"))
  expect_true(inherits(fit_model, "nlp_document_logreg_classifier_model"))
})

test_that("nlp_document_logreg_classifier ml_pipeline", {
  test_annotator <- nlp_document_logreg_classifier(pipeline, input_cols = c("stem"), output_col = "document_class",
                                                   label_column = "category")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("document_class" %in% colnames(transformed_data))
})

test_that("nlp_document_logreg_classifier tbl_spark", {
  transformed_data <- nlp_document_logreg_classifier(test_data, input_cols = c("stem"), output_col = "document_class",
                                                     label_column = "category")
  expect_true("document_class" %in% colnames(transformed_data))
})

