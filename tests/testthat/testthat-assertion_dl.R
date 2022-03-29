setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  train_data_file <- here::here("tests", "testthat", "data", "i2b2_assertion_sample.csv")
  train_data_frame <- sparklyr::spark_read_csv(sc, train_data_file)

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  chunk <- nlp_doc2chunk(sc, input_cols = c("document"), output_col = "chunk", chunk_col = "target", 
                         start_col = "start", start_col_by_token_index = TRUE, fail_on_missing = FALSE,
                         lowercase = TRUE)
  token <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "embeddings",
                                               name = "embeddings_clinical", remote_loc = "clinical/models")

  train_pipeline <- ml_pipeline(assembler, chunk, token, embeddings)
  train_data <- ml_fit_and_transform(train_pipeline, train_data_frame)
  
  sentence_detector <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  clinical_ner <- nlp_medical_ner_pretrained(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner",
                                        name = "ner_clinical", remote_loc = "clinical/models")
  ner_converter <- nlp_ner_converter(sc, input_cols = c("sentence", "token", "ner"), output_col = "ner_chunk")

  test_pipeline <- ml_pipeline(assembler, sentence_detector, tokenizer, embeddings,
                               clinical_ner, ner_converter)
  
  test_data <- ml_fit_and_transform(test_pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("train_pipeline", train_pipeline, envir = parent.frame())
  assign("train_data", train_data, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(train_pipeline, envir = .GlobalEnv)
  rm(train_data, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

# NO GETTER FOR REQUIRED PARAM labelCol
# test_that("assertion_dl param setting", {
#   test_args <- list(
#     input_cols = c("string1", "string2", "string3"),
#     output_col = "string1",
#     graph_folder = "/tmp",
#     config_proto_bytes = c(1, 2),
#     #label_column = "string1", # no getter
#     batch_size = 100,
#     epochs = 5,
#     #learning_rate = 0.01, # float
#     dropout = 0.5,
#     #max_sent_len = 10, # no getter
#     start_col = "string1",
#     end_col = "string1",
#     chunk_col = "string1",
#     enable_output_logs = TRUE,
#     output_logs_path = "string1",
#     validation_split = 0.2,
#     # verbose = "Epochs" # enum type
#   )
# 
#   test_param_setting(sc, nlp_assertion_dl, test_args)
# })

test_that("nlp_assertion_dl spark_connection", {
  test_annotator <- nlp_assertion_dl(sc, input_cols = c("document", "chunk", "embeddings"),
                                     output_col = "assertion", batch_size = 128, dropout = 0.1,
                                     learning_rate = 0.001, epochs = 50, validation_split = 0.2,
                                     start_col = "start", end_col = "end", max_sent_len = 250,
                                     scope_window = c(5, 10),
                                     enable_output_logs = TRUE, output_logs_path = "training_logs",
                                     graph_folder = here::here("tests", "testthat", "tf_graphs"))
  fit_model <- ml_fit(test_annotator, train_data)
  transformed_data <- ml_transform(fit_model, train_data)
  expect_true("assertion" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_assertion_dl"))
  expect_true(inherits(fit_model, "nlp_assertion_dl_model"))
})

test_that("nlp_assertion_dl ml_pipeline", {
  test_annotator <- nlp_assertion_dl(train_pipeline, input_cols = c("document", "chunk", "embeddings"),
                                     output_col = "assertion", batch_size = 128, dropout = 0.1,
                                     learning_rate = 0.001, epochs = 50, validation_split = 0.2,
                                     start_col = "start", end_col = "end", max_sent_len = 250,
                                     enable_output_logs = TRUE, output_logs_path = "training_logs",
                                     graph_folder = here::here("tests", "testthat", "tf_graphs"))
  transformed_data <- ml_fit_and_transform(test_annotator, train_data)
  expect_true("assertion" %in% colnames(transformed_data))
})

test_that("nlp_assertion_dl tbl_spark", {
  transformed_data <- nlp_assertion_dl(train_data, input_cols = c("document", "chunk", "embeddings"),
                                     output_col = "assertion", batch_size = 128, dropout = 0.1,
                                     learning_rate = 0.001, epochs = 50, validation_split = 0.2,
                                     start_col = "start", end_col = "end", max_sent_len = 250,
                                     enable_output_logs = TRUE, output_logs_path = "training_logs",
                                     graph_folder = here::here("tests", "testthat", "tf_graphs"))

  expect_true("assertion" %in% colnames(transformed_data))
})

test_that("nlp_assertion_dl pretrained", {
  model <- nlp_assertion_dl_pretrained(sc, input_cols = c("sentence", "ner_chunk", "embeddings"), output_col = "assertion",
                                       scope_window = c(5,10),
                                       name = "assertion_dl", remote_loc = "clinical/models")
  transformed_data <- ml_transform(model, test_data)
  expect_true("assertion" %in% colnames(transformed_data))

  expect_true(inherits(model, "nlp_assertion_dl_model"))
})

