setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_data(data.frame(text = "The cat ate the mouse"), "textdata")
  
  assign("sc", sc, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  sparklyr::tbl_uncache(sc, "textdata")
  rm(sc, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
})

test_that("nlp_document_assembler() param setting", {
  test_args <- list(
    input_col = "text", 
    output_col = "document",
    id_col = "rowkey", 
    metadata_col = "met",
    cleanup_mode = "shrink")
  test_param_setting(sc, nlp_document_assembler, test_args)
})

test_that("nlp_document_assembler() spark_connection", {
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  transformed_data <- ml_transform(assembler, text_tbl)
  
  expect_true("document" %in% colnames(transformed_data))
})

test_that("nlp_document_assembler() ml_pipeline", {
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  pipeline <- ml_pipeline(assembler)
  
  transformed_data <- ml_fit_and_transform(pipeline, text_tbl)
  
  expect_true("document" %in% colnames(transformed_data))
})

test_that("nlp_document_assembler() tbl_spark", {
  transformed_data <- nlp_document_assembler(text_tbl, input_col = "text", output_col = "document")
  expect_true("document" %in% colnames(transformed_data))
})
