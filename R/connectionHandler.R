
#' createConnectionHandler
#'
#' Creates a connection handler object based on the provided connection details settings.
#'
#' @param connectionDetailsSettings A list of connection details settings to pass directly to DatabaseConnector::createConnectionDetails.
#' @param tempEmulationSchema The temporary emulation schema (optional).
#' @param usePooledConnection Logical indicating whether to use a pooled connection (default is FALSE).
#' @param ... Additional arguments to be passed to the connection handler object.
#'
#' @importFrom checkmate assertList assertCharacter assertLogical
#' @importFrom Eunomia getEunomiaConnectionDetails
#' @importFrom rlang exec
#' @importFrom DatabaseConnector createConnectionDetails
#' @importFrom ResultModelManager PooledConnectionHandler
#' @importFrom tmp_ConnectionHandler tmp_ConnectionHandler
#'
#' @return A connection handler object.
#'
#' @export
ResultModelManager_createConnectionHandler  <- function(
    connectionDetailsSettings,
    tempEmulationSchema = NULL,
    usePooledConnection = FALSE,
    ...
){

  #
  # Check parameters
  #
  checkmate::assertList(connectionDetailsSettings)
  checkmate::assertCharacter(tempEmulationSchema, null.ok = TRUE)
  checkmate::assertLogical(usePooledConnection, null.ok = TRUE)

  #
  # function
  #

  if(connectionDetailsSettings$dbms == "eunomia"){
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  }else{
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!connectionDetailsSettings)
  }

  # set tempEmulationSchema if in config
  if(!is.null(tempEmulationSchema)){
    options(sqlRenderTempEmulationSchema = tempEmulationSchema)
  }


  if (usePooledConnection) {
    stop("not implemented")
    connectionHandler <- ResultModelManager::PooledConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
  } else {
    connectionHandler <- tmp_ConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
  }

  return(connectionHandler)

}

