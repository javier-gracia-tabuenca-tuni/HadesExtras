

test_that("createConnectionHandler works", {

  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = testSelectedConfiguration$connection$connectionDetailsSettings,
    tempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema
  )

  CDMdb <- CDMdbHandler$new(
    databaseName = testSelectedConfiguration$databaseName,
    connectionHandler = connectionHandler,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = testSelectedConfiguration$cdm$vocabularyDatabaseSchema
  )

  on.exit({CDMdb$finalize()})

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$databaseName |> checkmate::assertString()
  CDMdb$connectionStatusLog |> checkmate::expect_tibble()
  CDMdb$connectionStatusLog |> dplyr::filter(type != "SUCCESS") |> nrow() |>  expect_equal(0)
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")

})
