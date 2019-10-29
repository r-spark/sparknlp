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

test_that("lemmatizer param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1"
    #dictionary_path = "string1",
    #dictionary_key_delimiter = "string1",
    #dictionary_value_delimiter = "string1",
    #dictionary_read_as = "LINE_BY_LINE",
    #dictionary_options = list("option1" = "value1")
  )

  test_param_setting(sc, nlp_lemmatizer, test_args)
})

test_that("nlp_lemmatizer spark_connection", {
  test_annotator <- nlp_lemmatizer(sc, input_cols = c("token"), output_col = "lemma", 
                                   dictionary_path = here::here("tests", "testthat", "data", "lemmas_small.txt"))
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("lemma" %in% colnames(transformed_data))
})

test_that("nlp_lemmatizer ml_pipeline", {
  test_annotator <- nlp_lemmatizer(pipeline, input_cols = c("token"), output_col = "lemma", 
                                   dictionary_path = here::here("tests", "testthat", "data", "lemmas_small.txt"))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("lemma" %in% colnames(transformed_data))
})

test_that("nlp_lemmatizer tbl_spark", {
  fit_data <- nlp_lemmatizer(test_data, input_cols = c("token"), output_col = "lemma", 
                                     dictionary_path = here::here("tests", "testthat", "data", "lemmas_small.txt"))
  transformed_data <- ml_transform(fit_data, test_data)
  expect_true("lemma" %in% colnames(transformed_data))
})

test_that("nlp_lemmatizer pretrained", {
  model <- nlp_lemmatizer_pretrained(sc, input_cols = c("token"), output_col = "lemma", name = "lemma_antbnc")
  transformed_data <- ml_transform(model, test_data)
  expect_true("lemma" %in% colnames(transformed_data))
})

