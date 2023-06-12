

test_that("createConnectionHandler works", {

  testSelectedConfiguration  <- getOption("testSelectedConfiguration")

  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = testSelectedConfiguration$connection$connectionDetailsSettings,
    tempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema
  )

  CDMdb <- CDMdbHandler$new(
    connectionHandler = connectionHandler,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = testSelectedConfiguration$cdm$vocabularyDatabaseSchema
  )

  CDMdb |> checkmate::expect_class("CDMdbHandler")
  CDMdb$connectionStatusLog |> checkmate::expect_class("LogTibble")
  CDMdb$connectionStatusLog$logTibble |> dplyr::filter(type != "INFO") |> nrow() |>  expect_equal(0)
  CDMdb$getTblCDMSchema$person() |> checkmate::expect_class("tbl_dbi")
  CDMdb$getTblVocabularySchema$vocabulary() |> checkmate::expect_class("tbl_dbi")

})
