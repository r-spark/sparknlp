setup({
  sc <- testthat_spark_connection()
  assign("sc", sc, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
})

test_that("nlp_load_embeddings", {
  embeddings_path <- here::here("tests", "testthat", "data", "random_embeddings_dim4.txt")
  embeddings <- nlp_load_embeddings(sc, path = embeddings_path, format = "TEXT", dims = 4, reference = "embeddings_ref")
  dimension <- invoke(embeddings, "dim")
  expect_equal(dimension, 4)
})

test_that("nlp_save_embeddings", {
  save_path <- paste0(tempdir(), "/embeddings.txt")
  
  embeddings_path <- here::here("tests", "testthat", "data", "random_embeddings_dim4.txt")
  embeddings <- nlp_load_embeddings(sc, path = embeddings_path, format = "TEXT", dims = 4, reference = "embeddings_ref")
  nlp_save_embeddings(sc, save_path, embeddings)
  expect_true(file.exists(save_path))
})