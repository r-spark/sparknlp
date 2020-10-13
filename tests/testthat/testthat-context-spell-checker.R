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

test_that("context_spell_checker param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    batch_size = 100,
    #blacklist_min_freq = 3,
    case_strategy = 1,
    class_threshold = 0.4,
    epochs = 1000,
    error_threshold = 0.5,
    final_learning_rate = 0.01,
    initial_learning_rate = 0.005,
    lm_classes = 3,
    lazy_annotator = FALSE,
    max_candidates = 5,
    max_window_len = 3,
    min_count = 2,
    tradeoff = 0.1,
    validation_fraction = 0.2,
    weights = "string1",
    word_max_dist = 1
  )

  test_param_setting(sc, nlp_context_spell_checker, test_args)
})

test_that("nlp_context_spell_checker spark_connection", {
  test_annotator <- nlp_context_spell_checker(sc, input_cols = c("token"), output_col = "spell", lm_classes = 1400)
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("spell" %in% colnames(transformed_data))
 
  expect_true(inherits(test_annotator, "nlp_context_spell_checker"))
  expect_true(inherits(fit_model, "nlp_context_spell_checker_model"))
  
  # Test Float parameters
  oldvalue <- ml_param(test_annotator, "error_threshold")
  newmodel <- nlp_set_param(test_annotator, "error_threshold", 5)
  newvalue <- ml_param(newmodel, "error_threshold")
  
  expect_false(oldvalue == newvalue)
  expect_equal(newvalue, 5)
})

test_that("nlp_context_spell_checker ml_pipeline", {
 test_annotator <- nlp_context_spell_checker(pipeline, input_cols = c("token"), output_col = "spell", lm_classes = 1400)
 transformed_data <- ml_fit_and_transform(test_annotator, test_data)
 expect_true("spell" %in% colnames(transformed_data))
})

test_that("nlp_context_spell_checker tbl_spark", {
 transformed_data <- nlp_context_spell_checker(test_data, input_cols = c("token"), output_col = "spell", lm_classes = 1400)
 expect_true("spell" %in% colnames(transformed_data))
})

test_that("nlp_context_spell_checker pretrained", {
  model <- nlp_context_spell_checker_pretrained(sc, input_cols = c("token"), output_col = "spell")
  transformed_data <- ml_transform(model, test_data)
  expect_true("spell" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_context_spell_checker_model"))
})
