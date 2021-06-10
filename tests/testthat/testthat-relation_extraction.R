setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  train_data <- spark_read_parquet(sc, here::here("tests", "testthat", "data", "re_train.parquet"))

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "sentence", output_col = "sentences")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentences"), output_col = "tokens")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("sentences", "tokens"), output_col = "embeddings",
                                               name = "embeddings_clinical", remote_loc = "clinical/models")
  pos_tagger <- nlp_perceptron_pretrained(sc, input_cols = c("sentences", "tokens"), output_col = "pos_tags",
                                          name = "pos_clinical", remote_loc = "clinical/models")
  dependency_parser <- nlp_dependency_parser_pretrained(sc, input_cols = c("sentences", "pos_tags", "tokens"), 
                                                        output_col = "dependencies",
                                                        name = "dependency_conllu")

  pipeline <- ml_pipeline(assembler, tokenizer, embeddings, pos_tagger, dependency_parser)
  test_data <- ml_fit_and_transform(pipeline, train_data)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
  assign("train_data", train_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
  rm(train_data, envir = .GlobalEnv)
})

test_that("relation_extraction param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3", "string4"),
    output_col = "string5",
    batch_size = 100,
    dropout = 0.8,
    #epochs_number = 5, no getter
    feature_scaling = "string6",
    fix_imbalance = TRUE,
    from_entity_begin_col = "string7",
    from_entity_end_col = "string8",
    from_entity_label_col = "string9",
    label_col = "string10",
    learning_rate = 0.1,
    model_file = "string11",
    output_logs_path = "string12",
    to_entity_begin_col = "string13",
    to_entity_end_col = "string14",
    to_entity_label_col = "string15",
    validation_split = 0.2
  )

  test_param_setting(sc, nlp_relation_extraction, test_args)
})

test_that("nlp_relation_extraction spark_connection", {
  test_annotator <- nlp_relation_extraction(sc, input_cols = c("embeddings", "pos_tags", "train_ner_chunks", "dependencies"),
                                            output_col = "relations_t", label_col = "rel",
                                            epochs_number = 70, batch_size = 200, learning_rate = 0.001,
                                            model_file = here::here("tests", "testthat", "tf_graphs", "RE_in1200D_out20.pb"),
                                            fix_imbalance = TRUE, 
                                            from_entity_begin_col = "begin1i", from_entity_end_col = "end1i",
                                            from_entity_label_col = "label1", to_entity_begin_col = "begin2i",
                                            to_entity_end_col = "end2i", to_entity_label_col = "label2")
  
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("relations_t" %in% colnames(transformed_data))

  expect_true(inherits(test_annotator, "nlp_relation_extraction"))
  expect_true(inherits(fit_model, "nlp_relation_extraction_model"))
})

test_that("nlp_relation_extraction ml_pipeline", {
  test_annotator <- nlp_relation_extraction(sc, input_cols = c("embeddings", "pos_tags", "train_ner_chunks", "dependencies"),
                                            output_col = "relations_t", label_col = "rel",
                                            epochs_number = 70, batch_size = 200, learning_rate = 0.001,
                                            model_file = here::here("tests", "testthat", "tf_graphs", "RE_in1200D_out20.pb"),
                                            fix_imbalance = TRUE, 
                                            from_entity_begin_col = "begin1i", from_entity_end_col = "end1i",
                                            from_entity_label_col = "label1", to_entity_begin_col = "begin2i",
                                            to_entity_end_col = "end2i", to_entity_label_col = "label2")

  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("relations_t" %in% colnames(transformed_data))
})

test_that("nlp_relation_extraction tbl_spark", {
  transformed_data <- nlp_relation_extraction(test_data, input_cols = c("embeddings", "pos_tags", "train_ner_chunks", "dependencies"),
                                            output_col = "relations_t", label_col = "rel",
                                            epochs_number = 70, batch_size = 200, learning_rate = 0.001,
                                            model_file = here::here("tests", "testthat", "tf_graphs", "RE_in1200D_out20.pb"),
                                            fix_imbalance = TRUE, 
                                            from_entity_begin_col = "begin1i", from_entity_end_col = "end1i",
                                            from_entity_label_col = "label1", to_entity_begin_col = "begin2i",
                                            to_entity_end_col = "end2i", to_entity_label_col = "label2")

  expect_true("relations_t" %in% colnames(transformed_data))
})

test_that("nlp_relation_extraction pretrained", {
  model <- nlp_relation_extraction_pretrained(sc, input_cols = c("embeddings", "pos_tags", "train_ner_chunks", "dependencies"), 
                                              output_col = "relations",
                                              name = "re_bodypart_directions", remote_loc = "clinical/models",
                                              prediction_threshold = 0.75, max_syntactic_distance = 4,
                                              relation_pairs = c("direction-external_body_part_or_region"))
  
  transformed_data <- ml_transform(model, test_data)
  expect_true("relations" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_relation_extraction_model"))
})

