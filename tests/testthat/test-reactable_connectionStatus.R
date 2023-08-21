


test_that("reactable_connectionStatus works", {

  log <- HadesExtras::LogTibble$new()
  log$INFO("step 1", "example info")
  log$WARNING("step 2", "example warning")
  log$ERROR("step 3", "example error")
  log$SUCCESS("step 3", "example success")

  connectionStatusLogs <- log$logTibble |>
    dplyr::mutate(databaseName="Database name")

  reactableResult <- reactable_connectionStatus(connectionStatusLogs)

  reactableResult |> checkmate::expect_class(classes = c("reactable", "htmlwidget"))
  reactableResult$x$tag$attribs$columns |> length() |> expect_equal(4)
})
