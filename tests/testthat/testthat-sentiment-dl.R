setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  train_data_file <- here::here("tests", "testthat", "data", "sentiment.csv")
  train_data <- spark_read_csv(sc, train_data_file)

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  word_embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "embeddings", name = "glove_100d")
  sentence_embeddings <- nlp_sentence_embeddings(sc, input_cols = c("document", "embeddings"), output_col = "sentence_embeddings", pooling_strategy = "AVERAGE", storage_ref = "glove_100d")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, word_embeddings, sentence_embeddings)
  test_data <- ml_fit_and_transform(pipeline, train_data)

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

test_that("sentiment_dl param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    label_col = "string1",
    max_epochs = 5,
    lr = 0.1,
    batch_size = 100,
    dropout = 0.5,
    verbose = 0,
    validation_split = 0.2,
    threshold = 0.3,
    threshold_label = "string2",
    enable_output_logs = TRUE,
    output_logs_path = "string1"
  )

  test_param_setting(sc, nlp_sentiment_dl, test_args)
})

test_that("nlp_sentiment_dl spark_connection", {
  test_annotator <- nlp_sentiment_dl(sc, input_cols = c("sentence_embeddings"), output_col = "sentiment",
                                     label_col = "label", max_epochs = 5, enable_output_logs = TRUE)
  fit_model <- ml_fit(test_annotator, test_data)
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "sentiment")
  
  expect_true(inherits(test_annotator, "nlp_sentiment_dl"))
  expect_true(inherits(fit_model, "nlp_sentiment_dl_model"))
  
  # Test Float parameters
  oldvalue <- ml_param(test_annotator, "lr")
  newmodel <- nlp_set_param(test_annotator, "lr", 0.8)
  newvalue <- ml_param(newmodel, "lr")
  
  expect_false(oldvalue == newvalue)
  expect_equal(newvalue, 0.8)
})

test_that("nlp_sentiment_dl ml_pipeline", {
  test_annotator <- nlp_sentiment_dl(pipeline, input_cols = c("sentence_embeddings"), output_col = "sentiment", label_col = "label")
  fit_pipeline <- ml_fit(test_annotator, train_data)
  transformed_data <- ml_transform(fit_pipeline, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
})

test_that("nlp_sentiment_dl tbl_spark", {
  fit_model <- nlp_sentiment_dl(test_data, input_cols = c("sentence_embeddings"), output_col = "sentiment", label_col = "label")
  expect_equal(invoke(spark_jobj(fit_model), "getOutputCol"), "sentiment")
})

test_that("nlp_sentiment_dl pretrained", {
  model <- nlp_sentiment_dl_pretrained(sc, input_cols = c("sentence_embeddings"), output_col = "sentiment",
                                       name = "sentimentdl_glove_imdb")
  transformed_data <- ml_transform(model, test_data)
  expect_true("sentiment" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_sentiment_dl_model"))
})

test_that("nlp_get classes for SentimentDL model", {
  model <- nlp_sentiment_dl_pretrained(sc, input_cols = c("sentence_embeddings"), output_col = "sentiment")
  classes <- nlp_get_classes(model)
  expect_equal(sort(unlist(classes)), c("negative", "positive"))
})
# 
