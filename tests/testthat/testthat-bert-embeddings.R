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

test_that("bert_embeddings param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    batch_size = 100,
    case_sensitive = FALSE,
    config_proto_bytes = c(1,3),
    dimension = 300,
    max_sentence_length = 10,
    pooling_layer = 0
  )

  test_param_setting(sc, nlp_bert_embeddings, test_args)
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

# test_that("nlp_bert_embeddings spark_connection", {
#   test_annotator <- nlp_bert_embeddings(sc, input_cols = c("sentence", "token"), output_col = "bert")
#   transformed_data <- ml_transform(test_annotator, test_data)
#   expect_true("bert" %in% colnames(transformed_data))
# })
# 
# test_that("nlp_bert_embeddings ml_pipeline", {
#   test_annotator <- nlp_bert_embeddings(pipeline, input_cols = c("sentence", "token"), output_col = "bert")
#   transformed_data <- ml_fit_and_transform(test_annotator, test_data)
#   expect_true("bert" %in% colnames(transformed_data))
# })
# 
# test_that("nlp_bert_embeddings tbl_spark", {
#   transformed_data <- nlp_bert_embeddings(test_data, input_cols = c("sentence", "token"), output_col = "bert")
#   expect_true("bert" %in% colnames(transformed_data))
# })

