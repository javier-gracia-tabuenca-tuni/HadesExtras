
devtools::load_all(".")

log <- HadesExtras::LogTibble$new()
log$INFO("step 1", "example info")
log$WARNING("step 2", "example warning")
log$ERROR("step 3", "example error")

connectionStatusLogs <- log$logTibble |>
  dplyr::mutate(databaseName="Database name")

reactable_connectionStatus(connectionStatusLogs)
