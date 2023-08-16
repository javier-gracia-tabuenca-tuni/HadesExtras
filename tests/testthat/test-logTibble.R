#
# LogTibble
#
test_that("LogTibble works", {
  log_tibble <- LogTibble$new()

  # Test initialization
  expect_true(is(log_tibble, "LogTibble"))

  # Test adding log messages
  log_tibble$INFO("Step 1", "This is an informational message.")
  log_tibble$WARNING("Step 2", "This is a warning message.")
  log_tibble$ERROR("Step 3", "This is an error message.")
  log_tibble$SUCCESS("Step 4", "This is an success message.")

  # Test log tibble content
  log <- log_tibble$logTibble
  expect_equal(nrow(log), 4)
  expect_identical(log$type, factor(c("INFO", "WARNING", "ERROR", "SUCCESS"), levels = c("INFO", "WARNING", "ERROR", "SUCCESS")))
  expect_equal(log$step, c("Step 1", "Step 2", "Step 3", "Step 4"))
  expect_equal(log$message, c(
    "This is an informational message.",
    "This is a warning message.",
    "This is an error message.",
    "This is an success message."
  ))
})
