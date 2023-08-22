
helper_createNewConnection <- function(){
  connectionDetailsSettings <- testSelectedConfiguration$connection$connectionDetailsSettings

  if(connectionDetailsSettings$dbms == "eunomia"){
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  }else{
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!connectionDetailsSettings)
  }

  connection <- DatabaseConnector::connect(connectionDetails)

  return(connection)
}


helper_getParedSourcePersonAndPersonIds  <- function(
    connection,
    cohortDatabaseSchema,
    numberPersons){

  # Connect, collect tables
  personTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, "person"))

  # get first n persons
  pairedSourcePersonAndPersonIds  <- personTable  |>
    dplyr::arrange(person_id) |>
    dplyr::select(person_id, person_source_value) |>
    dplyr::collect(n=numberPersons)


  return(pairedSourcePersonAndPersonIds)
}



helper_createNewCohortTableHandler <- function(){
  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = testSelectedConfiguration$connection$connectionDetailsSettings,
    tempEmulationSchema = testSelectedConfiguration$connection$tempEmulationSchema
  )
  cohortTableHandler <- CohortTableHandler$new(
    connectionHandler = connectionHandler,
    databaseName = testSelectedConfiguration$databaseName,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = testSelectedConfiguration$cdm$vocabularyDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableName = testSelectedConfiguration$cohortTable$cohortTableName
  )
  return(cohortTableHandler)
}
