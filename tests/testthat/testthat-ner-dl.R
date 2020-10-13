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
  word_embeddings <- nlp_word_embeddings_pretrained(sc, output_col = "embeddings")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, word_embeddings)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("train_data", train_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
  rm(train_data, envir = .GlobalEnv)
})

test_that("ner_dl param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    label_col = "string1",
    max_epochs = 5,
    lr = 0.1,
    po = 0.1,
    batch_size = 100,
    dropout = 0.5,
    verbose = 0,
    include_confidence = TRUE,
    random_seed = 100,
    graph_folder = "folder1",
    validation_split = 0.2,
    eval_log_extended = TRUE,
    enable_output_logs = TRUE,
    output_logs_path = "string1"
  )

  test_param_setting(sc, nlp_ner_dl, test_args)
})

test_that("nlp_ner_dl spark_connection", {
  test_annotator <- nlp_ner_dl(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner", label_col = "label")
  fit_model <- ml_fit(test_annotator, train_data)
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "ner")
  
  expect_true(inherits(test_annotator, "nlp_ner_dl"))
  expect_true(inherits(fit_model, "nlp_ner_dl_model"))
  
  # Test Float parameters
  oldvalue <- ml_param(test_annotator, "validation_split")
  newmodel <- nlp_set_param(test_annotator, "validation_split", 0.8)
  newvalue <- ml_param(newmodel, "validation_split")
  
  expect_false(oldvalue == newvalue)
  expect_equal(newvalue, 0.8)
})

test_that("nlp_ner_dl ml_pipeline", {
  test_annotator <- nlp_ner_dl(pipeline, input_cols = c("sentence", "token", "embeddings"), output_col = "ner", label_col = "label")
  fit_pipeline <- ml_fit(test_annotator, train_data)
  transformed_data <- ml_transform(fit_pipeline, test_data)
  expect_true("ner" %in% colnames(transformed_data))
})

test_that("nlp_ner_dl tbl_spark", {
  fit_model <- nlp_ner_dl(train_data, input_cols = c("sentence", "token", "embeddings"), output_col = "ner", label_col = "label")
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "ner")
})

test_that("nlp_ner_dl pretrained", {
  model <- nlp_ner_dl_pretrained(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner")
  transformed_data <- ml_transform(model, test_data)
  expect_true("ner" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_ner_dl_model"))
})

test_that("nlp_ner_dl classes", {
  model <- nlp_ner_dl_pretrained(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner")
  classes <- nlp_get_classes(model)
  expect_equal(sort(unlist(classes)), c("B-LOC", "B-MISC", "B-ORG", "B-PER", "I-LOC", "I-MISC", "I-ORG", "I-PER", "O"))
})

test_that("nlp_get_classes for NerDLModel", {
  model <- nlp_ner_dl_pretrained(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner")
  classes <- nlp_get_classes(model)
  expect_equal(sort(unlist(classes)), c("B-LOC", "B-MISC", "B-ORG", "B-PER", "I-LOC", "I-MISC", "I-ORG", "I-PER", "O"))
})
