




logTibble_NewLog  <- function(){
    log <- tibble::tibble(
        type = factor(NA, levels = c("INFO", "WARNING", "ERROR")),
        step = character(0),
        message = character(0),
        .rows = 0L
    )
    return(log)
}

logTibble_INFO  <- function(log, step, message, ...){
    return(.logTibble_add(log, "INFO", step, message, ...))
}

logTibble_WARNING  <- function(log, step, message, ...){
    return(.logTibble_add(log, "WARNING", step, message, ...))
}

logTibble_ERROR  <- function(log, step, message, ...){
    return(.logTibble_add(log, "ERROR", step, message, ...))
}

.logTibble_add  <- function(log, type, step, message, ...){
    log <- log  |>
        dplyr::add_row(
            type = factor(type, levels = c("INFO", "WARNING", "ERROR")),
            step = step,
            message = paste(message, ...)
        )
    return(log)
}
