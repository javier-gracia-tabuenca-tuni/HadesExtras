
#
# ResultModelManager_createConnectionHandler
#
test_that("ResultModelManager_createConnectionHandler works", {

  testSelectedConfiguration  <- getOption("testSelectedConfiguration")

  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = testSelectedConfiguration$connection$connectionDetailsSettings,
    tempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema
  )

  connectionHandler |> checkmate::expect_class("ConnectionHandler")

  connectionHandler$finalize()

})