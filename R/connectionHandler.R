



createConnectionHandler  <- function(
    connectionDetailsSettings,
    tempEmulationSchema = NULL,
    usePooledConnection = FALSE,
){

  #
  # Check parameters
  #
    checkmate::assertList(connectionDetailsSettings)

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
    options(sqlRenderTempEmulationSchema = database_settings$schemas$tempEmulationSchema)
  }


   if (usePooledConnection) {
    stop("not implemented")
    connectionHandler <- ResultModelManager::PooledConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
    } else {
    connectionHandler <- tmp_ConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
    }

   return(connectionHandler)

}



getConnectionHanderConnectionStatus  <- function(
    connectionHandler
){

    connectionStatus <- logTibble_NewLog()

    # Check connection
      errorMessage <- ""
      tryCatch({
        connectionHandler$initConnection()
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" | !connectionHandler$dbIsValid()){
        connectionStatus <- logTibble_ERROR(connectionStatus, "Database Connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "Database Connection", "Valid connection")
      }

    # TODO: Check can create temp tables in tempEmulationSchema
       errorMessage <- ""
       tryCatch({
            connectionHandler$getConnection()  |>
                dplyr::copy_to(cars, overwrite = TRUE)
            DatabaseConnector::dropEmulatedTempTables(connection)
        }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_WARNING(connectionStatus, "temp table creation", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "temp table creation", "can create temp tables")
      }

}
