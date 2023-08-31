
#' createConnectionHandler
#'
#' Creates a connection handler object based on the provided connection details settings.
#'
#' @param connectionDetailsSettings A list of connection details settings to pass directly to DatabaseConnector::createConnectionDetails.
#' @param tempEmulationSchema The temporary emulation schema (optional).
#' @param usePooledConnection Logical indicating whether to use a pooled connection (default is FALSE).
#' @param useBigrqueryUpload Logical indicating whether to use fast table upload for bigquery (default is FALSE)
#' @param ... Additional arguments to be passed to the connection handler object.
#'
#' @importFrom checkmate assertList assertCharacter assertLogical
#' @importFrom Eunomia getEunomiaConnectionDetails
#' @importFrom rlang exec
#' @importFrom DatabaseConnector createConnectionDetails
#' @importFrom ResultModelManager PooledConnectionHandler
#'
#' @return A connection handler object.
#'
#' @export
ResultModelManager_createConnectionHandler  <- function(
    connectionDetailsSettings,
    tempEmulationSchema = NULL,
    useBigrqueryUpload = NULL,
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
  }else{
    options(sqlRenderTempEmulationSchema = NULL)
  }

  # set useBigrqueryUpload if in config
  if(!is.null(useBigrqueryUpload)){
    options(useBigrqueryUpload = useBigrqueryUpload)

    # bq authentication
    if(useBigrqueryUpload==TRUE){
      checkmate::assertTRUE(connectionDetails$dbms=="bigquery")

      options(gargle_oauth_cache=FALSE) #to avoid the question that freezes the app
      connectionString <- connectionDetails$connectionString()
      if( connectionString |> stringr::str_detect(";OAuthType=0;")){
        OAuthPvtKeyPath <- connectionString |>
          stringr::str_extract("OAuthPvtKeyPath=([:graph:][^;]+);") |>
          stringr::str_remove("OAuthPvtKeyPath=") |> stringr::str_remove(";")

        checkmate::assertFileExists(OAuthPvtKeyPath)
        bigrquery::bq_auth(path = OAuthPvtKeyPath)

      }else{
        bigrquery::bq_auth(scopes = "https://www.googleapis.com/auth/bigquery")
      }

      connectionDetails$connectionString
    }

  }else{
    options(useBigrqueryUpload = NULL)
  }


  if (usePooledConnection) {
    stop("not implemented")
    connectionHandler <- ResultModelManager::PooledConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
  } else {
    connectionHandler <- tmp_ConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
  }

  return(connectionHandler)

}

