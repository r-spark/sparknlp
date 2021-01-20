setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  train_data_file <- here::here("tests", "testthat", "data", "AskAPatient.fold-0.test.txt")
  train_data <- sparklyr::spark_read_csv(sc, "train", train_data_file, delimiter = "\t", 
                                         columns = c("conceptId", "_term", "term"))
  
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "term", output_col = "document")
  bert_emb <- nlp_bert_sentence_embeddings_pretrained(sc, input_cols = c("document"), output_col = "sentence_embeddings",
                                                      name = "sbiobert_base_cased_mli", remote_loc = "clinical/models")
  
  #chunk <- nlp_doc2chunk(sc, input_cols = c("document"), output_col = "chunk")
  #tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  #embeddings <- nlp_word_embeddings_pretrained(sc, name = "embeddings_clinical",
  #                                             remote_loc = "clinical/models", 
  #                                            input_cols = c("document", "token"), output_col = "embeddings")
  #sentence_emb <- nlp_sentence_embeddings(sc, input_cols = c("document", "embeddings"), output_col = "sentence_embeddings")
  
  pipeline <- ml_pipeline(assembler, bert_emb)
  test_data <- ml_fit_and_transform(pipeline, train_data)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("sentence_entity_resolver param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    label_column = NULL, # no getter or setter
    normalized_col = "string1",
    neighbors = 10,
    threshold = 0.4,
    miss_as_empty = TRUE,
    case_sensitive = FALSE,
    confidence_function = "string1",
    distance_function = "string1"
  )

  test_param_setting(sc, nlp_sentence_entity_resolver, test_args)
})

test_that("nlp_sentence_entity_resolver spark_connection", {
  test_annotator <- nlp_sentence_entity_resolver(sc, input_cols = c("sentence_embeddings"), 
                                                 output_col = "prediction", label_column = "conceptId")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("prediction" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_sentence_entity_resolver"))
  expect_true(inherits(fit_model, "nlp_sentence_entity_resolver_model"))
})

test_that("nlp_sentence_entity_resolver ml_pipeline", {
  test_annotator <- nlp_sentence_entity_resolver(pipeline, input_cols = c("sentence_embeddings"), output_col = "prediction", label_column = "conceptId")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("prediction" %in% colnames(transformed_data))
})

test_that("nlp_sentence_entity_resolver tbl_spark", {
  transformed_data <- nlp_sentence_entity_resolver(test_data, input_cols = c("sentence_embeddings"), output_col = "prediction", label_column = "conceptId")
  expect_true("prediction" %in% colnames(transformed_data))
})

test_that("nlp_sentence_entity_resolver pretrained", {
  model <- nlp_sentence_entity_resolver_pretrained(sc, input_cols = c("sentence_embeddings"), 
                                                output_col = "recognized",
                                                name = "sbiobertresolve_icd10cm", lang = "en", remote_loc = "clinical/models")
  transformed_data <- ml_transform(model, test_data)
  expect_true("recognized" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_sentence_entity_resolver_model"))
})
