#'
#' LogTibble
#'
#' @description
#' Class for managing log messages as a tibble.
#'
#' @importFrom R6 R6Class
#' @importFrom tibble tibble add_row
#' @importFrom dplyr add_row
#'
#' @export LogTibble
LogTibble <- R6::R6Class(
  classname = "LogTibble",
  private = list(
    log = NULL
  ),

  public = list(
    #' initialize
    #' @description
    #' Initializes a new LogTibble object.
    initialize = function() {
      private$log <- tibble::tibble(
        type = factor(NA, levels = c("INFO", "WARNING", "ERROR")),
        step = character(0),
        message = character(0),
        .rows = 0L
      )
    },

    #' addLog
    #' @description
    #' Adds a log message to the log tibble.
    #'
    #' @param type     Type of log message ("INFO", "WARNING", "ERROR")
    #' @param step     Step or description associated with the log message
    #' @param message  Log message content
    #' @param ...      Additional parameters for message formatting
    addLog = function(type, step, message, ...) {
      private$log <- private$log %>%
        dplyr::add_row(
          type = factor(type, levels = c("INFO", "WARNING", "ERROR")),
          step = step,
          message = paste(message, ...)
        )
    },

    #' addInfo
    #' @description
    #' Adds an informational log message to the log tibble.
    #'
    #' @param step     Step or description associated with the log message
    #' @param message  Log message content
    #' @param ...      Additional parameters for message formatting
    INFO = function(step, message, ...) {
      self$addLog("INFO", step, message, ...)
    },

    #' addWarning
    #' @description
    #' Adds a warning log message to the log tibble.
    #'
    #' @param step     Step or description associated with the log message
    #' @param message  Log message content
    #' @param ...      Additional parameters for message formatting
    WARNING = function(step, message, ...) {
      self$addLog("WARNING", step, message, ...)
    },

    #' addError
    #' @description
    #' Adds an error log message to the log tibble.
    #'
    #' @param step     Step or description associated with the log message
    #' @param message  Log message content
    #' @param ...      Additional parameters for message formatting
    ERROR = function(step, message, ...) {
      self$addLog("ERROR", step, message, ...)
    },

    print = function(){
      print(private$log)
    }
  ),

  active = list(
    #' logTibble
    #' @description
    #' Returns the log tibble.
    #'
    #' @param x  (read-only) Ignored argument for compatibility
    logTibble = function(x = NULL) {
      if (!is.null(x)) {
        stop("logTibble is read-only")
      }
      return(private$log)
    }
  )
)
