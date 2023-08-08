#' reactable_connectionStatus
#'
#' A function to display connection status in a reactable.
#'
#' @param connectionStatus A data frame containing connection status information.
#' @param emojiCheck An emoji to display for successful connections.
#' @param emojiWarning An emoji to display for warning connections.
#' @param emojiError An emoji to display for failed connections.
#'
#' @return A reactable displaying the connection status information.
#'
#' @importFrom reactable reactable
#' @importFrom dplyr mutate case_when
#'
#' @export
reactable_connectionStatus <- function(
    connectionStatus,
    emojiCheck = "\u2705",
    emojiWarning = "\u26A0\uFE0F",
    emojiError = "\u274c"
    ) {

  if(nrow(connectionStatus)==0){
    return(connectionStatus |>
             reactable::reactable())
  }

  connectionStatus |>
    dplyr::mutate(
      type = dplyr::case_when(
        type == "INFO" ~ emojiCheck,
        type == "WARNING" ~ emojiWarning,
        type == "ERROR" ~ emojiError
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


