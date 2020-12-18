setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  train_data_file <- here::here("tests", "testthat", "data", "crf-eng.train.small")
  conll_data <- nlp_conll_read_dataset(sc, train_data_file)
  embeddings <- nlp_word_embeddings_pretrained(sc, output_col = "embeddings")
  train_data <- ml_transform(embeddings, conll_data)
  
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  pos <- nlp_perceptron_pretrained(sc, input_cols = c("sentence", "token"), output_col = "pos")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, pos, embeddings)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

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
  rm(train_data, envir = .GlobalEnv)
})

test_that("nlp_ner_crf param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3", "string4"),
    output_col = "string1",
    label_col = "string1",
    min_epochs = 1,
    max_epochs = 10,
    l2 = 3.0,
    C0 = 12150000,
    loss_eps = 0.001,
    min_w = 0.6,
    entities = c("ORG"),
    verbose = 2,
    random_seed = 0
  )
  
  test_param_setting(sc, nlp_ner_crf, test_args)
})

test_that("nlp_ner_crf spark_connection", {
  test_annotator <- nlp_ner_crf(sc, input_cols = c("sentence", "token", "pos", "embeddings"), output_col = "ner", label_col = "label")
  fit_model <- ml_fit(test_annotator, train_data)
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "ner")
  
  expect_true(inherits(test_annotator, "nlp_ner_crf"))
  expect_true(inherits(fit_model, "nlp_ner_crf_model"))
})

test_that("nlp_nlp_ner_crf ml_pipeline", {
  test_annotator <- nlp_ner_crf(pipeline, input_cols = c("sentence", "token", "pos", "embeddings"), output_col = "ner", label_col = "label")
  fit_pipeline <- ml_fit(test_annotator, train_data)
  transformed_data <- ml_transform(fit_pipeline, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_ner_crf tbl_spark", {
  fit_model <- nlp_ner_crf(train_data, input_cols = c("sentence", "token", "pos", "embeddings"), output_col = "ner", label_col = "label")
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "ner")
})

test_that("nlp_ner_crf pretrained", {
  model <- nlp_ner_crf_pretrained(sc, input_cols = c("sentence", "token", "pos", "embeddings"), output_col = "ner")
  transformed_data <- ml_transform(model, test_data)
  expect_true("ner" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_ner_crf_model"))
})


