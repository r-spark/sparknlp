setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  train_data_file <- here::here("tests", "testthat", "data", "e2e.csv")
  csv_data <- spark_read_csv(sc, train_data_file) %>%
    dplyr::mutate(label = split(mr, ", ")) %>%
    dplyr::select(-mr, text = ref)


  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  word_embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "word_embeddings")
  sentence_embeddings <- nlp_sentence_embeddings(sc, input_cols = c("document", "word_embeddings"), output_col = "sentence_embeddings")

  use <- nlp_univ_sent_encoder_pretrained(sc, input_cols = c("document"), output_col = "sentence_embeddings")
  test_pipeline <- ml_pipeline(assembler, use)
  test_data <- ml_fit_and_transform(test_pipeline, text_tbl)

  pipeline <- ml_pipeline(assembler, tokenizer, word_embeddings, sentence_embeddings)
  train_data <- ml_fit_and_transform(pipeline, csv_data)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("train_data", train_data, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("multi_classifier_dl param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    batch_size = 1000,
    enable_output_logs = TRUE,
    label_col = "string1",
    lr = 0.01,
    max_epochs = 3,
    output_logs_path = "string1",
    shuffle_per_epoch = TRUE,
    threshold = 0.5,
    validation_split = 0.2,
    verbose = 2
  )

  test_param_setting(sc, nlp_multi_classifier_dl, test_args)
})

test_that("nlp_multi_classifier_dl spark_connection", {
  test_annotator <- nlp_multi_classifier_dl(sc, input_cols = c("sentence_embeddings"),
                                            output_col = "category", label_col = "label")
  fit_model <- ml_fit(test_annotator, train_data)
  transformed_data <- ml_transform(fit_model, train_data)
  expect_true("category" %in% colnames(transformed_data))

  expect_true(inherits(test_annotator, "nlp_multi_classifier_dl"))
  expect_true(inherits(fit_model, "nlp_multi_classifier_dl_model"))
})

test_that("nlp_multi_classifier_dl ml_pipeline", {
  test_annotator <- nlp_multi_classifier_dl(pipeline, input_cols = c("sentence_embeddings"),
                                            output_col = "category", label_col = "label")
  transformed_data <- ml_fit_and_transform(test_annotator, train_data)
  expect_true("category" %in% colnames(transformed_data))
})

test_that("nlp_multi_classifier_dl tbl_spark", {
  transformed_data <- nlp_multi_classifier_dl(train_data, input_cols = c("sentence_embeddings"),
                                              output_col = "category", label_col = "label")
  expect_true("category" %in% colnames(transformed_data))
})

test_that("nlp_multi_classifier_dl pretrained", {
  model <- nlp_multi_classifier_dl_pretrained(sc, input_cols = c("sentence_embeddings"), output_col = "category")
  transformed_data <- ml_transform(model, test_data)
  expect_true("category" %in% colnames(transformed_data))

  expect_true(inherits(model, "nlp_multi_classifier_dl_model"))
})

