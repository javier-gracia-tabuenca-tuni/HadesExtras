#' reactable_connectionStatus
#'
#' A function to display connection status in a reactable.
#'
#' @param connectionStatus A data frame containing connection status information.
#' @param emojis list of emojis
#'
#' @return A reactable displaying the connection status information.
#'
#' @importFrom reactable reactable
#' @importFrom dplyr mutate case_when
#'
#' @export
reactable_connectionStatus <- function(
    connectionStatus,
    emojis = list(
      error = "\u274c",
      warning = "\u26A0\uFE0F",
      success = "\u2705",
      info = '\u2139\uFE0F'
    )
    ) {

  if(nrow(connectionStatus)==0){
    return(connectionStatus |>
             reactable::reactable())
  }

  connectionStatus |>
    dplyr::mutate(
      type = dplyr::case_when(
        type == "INFO" ~ emojis$info,
        type == "WARNING" ~ emojis$warning,
        type == "ERROR" ~ emojis$error,
        type == "SUCCESS" ~ emojis$success
      )
    ) |>
    reactable::reactable(
      groupBy = "databaseName",
      columns = list(
        databaseName = reactable::colDef(
          name = "Database"
        ),
        type = reactable::colDef(
          name = "Status",
          aggregate = "frequency"
        ),
        step = reactable::colDef(
          name = "Check"
        ),
        message = reactable::colDef(
          name = "Message"
        )
      )
    )
}


