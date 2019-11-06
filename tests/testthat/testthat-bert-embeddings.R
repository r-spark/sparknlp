setup({
  sc <- testthat_spark_connection()
  # config <- spark_config()
  # config$`sparklyr.shell.driver-memory` <- "8G"
  # sc <- spark_connect(master = "local", version = "2.4.3", config = config)
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
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("nlp_bert_embeddings pretrained", {
  model <- nlp_bert_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "bert")
  transformed_data <- ml_transform(model, test_data)
  expect_true("bert" %in% colnames(transformed_data))
})

test_that("nlp_bert_embeddings load", {
  model <- ml_load(sc, "~/cache_pretrained/bert_base_cased_en_2.2.0_2.4_1566671427398")
  transformed_data <- ml_transform(model, test_data)
  expect_true("bert" %in% colnames(transformed_data))
})
