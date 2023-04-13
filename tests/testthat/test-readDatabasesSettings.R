library(testthat)
library(checkmate)



test_that("readDatabaseSettings works with correct values", {

  path_databases_settings_yalm <- testthat::test_path("databases_settings", "databases_settings.yml")

  r <- readDatabasesSettings(path_databases_settings_yalm)
  r |>   expect_tibble(types = c("character", "list", "character"))
  r |> dplyr::pull(connection_errors) |> expect_equal(c("",""))

})


test_that("readDatabaseSettings errors with wrong connection parameters", {

  path_databases_settings_yalm <- testthat::test_path("databases_settings", "databases_settings_wrong_connection.yml")

  r <- readDatabasesSettings(path_databases_settings_yalm)
  r |>   expect_tibble(types = c("character", "list", "character"))
  r |> dplyr::pull(connection_errors) |> expect_match("Error in connection settings:")

})

test_that("readDatabaseSettings errors with wrong schema", {

  path_databases_settings_yalm <- testthat::test_path("databases_settings", "databases_settings_wrong_schema.yml")

  r <- readDatabasesSettings(path_databases_settings_yalm)
  r |>   expect_tibble(types = c("character", "list", "character"))
  r |> dplyr::pull(connection_errors) |> expect_match("Could not connect to CDM schema Error")

})

test_that("readDatabaseSettings errors with wrong webapi url ", {

  path_databases_settings_yalm <- testthat::test_path("databases_settings", "databases_settings_wrong_webapi_url.yml")

  r <- readDatabasesSettings(path_databases_settings_yalm)
  r |>   expect_tibble(types = c("character", "list", "character"))
  r |> dplyr::pull(connection_errors) |> expect_match("Could not connect to webAPI Error")

})

test_that("readDatabaseSettings errors with wrong webapi sourcekey", {

  path_databases_settings_yalm <- testthat::test_path("databases_settings", "databases_settings_wrong_webapi_sourcekey.yml")

  r <- readDatabasesSettings(path_databases_settings_yalm)
  r |>   expect_tibble(types = c("character", "list", "character"))
  r |> dplyr::pull(connection_errors) |> expect_match("Invalid sourceKey:")

})
