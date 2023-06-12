
test_that("LogTibble class", {
  log_tibble <- LogTibble$new()

  # Test initialization
  expect_true(is(log_tibble, "LogTibble"))

  # Test adding log messages
  log_tibble$INFO("Step 1", "This is an informational message.")
  log_tibble$WARNING("Step 2", "This is a warning message.")
  log_tibble$ERROR("Step 3", "This is an error message.")

  # Test log tibble content
  log <- log_tibble$logTibble
  expect_equal(nrow(log), 3)
  expect_identical(log$type, factor(c("INFO", "WARNING", "ERROR"), levels = c("INFO", "WARNING", "ERROR")))
  expect_equal(log$step, c("Step 1", "Step 2", "Step 3"))
  expect_equal(log$message, c(
    "This is an informational message.",
    "This is a warning message.",
    "This is an error message."
  ))
})