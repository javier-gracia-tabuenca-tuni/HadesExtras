

#' readDatabasesNames
#'
#' Return database names available in a databases settings yalm file
#'
#' @param path_databases_settings_yalm path to the databases settings yalm file
#'
#' @return database names
#' @export
#'
#' @importFrom checkmate assertFileExists assertList
#' @importFrom yaml read_yaml
readDatabasesNames <- function(path_databases_settings_yalm) {

  #
  # Check parameters
  #

  checkmate::assertFileExists(path_databases_settings_yalm, extension = "yml")
  databases_settings <- yaml::read_yaml(path_databases_settings_yalm)
  checkmate::assertList(databases_settings, names = "named")

  #
  # function
  #

  databases_names <- databases_settings |> names()

  return(databases_names)
}




#' Title readDatabaseSettings
#'
#' Returns the database settings for the `database_name` in the databases settings yalm file.
#' Rewrites the connectionDetails using  DatabaseConnector::createConnectionDetails
#'
#' @param path_databases_settings_yalm path to the databases settings yalm file
#' @param database_name a valid database name in the databases settings yalm file
#'
#' @return database settings
#' @export
#'
#' @importFrom checkmate assertFileExists assertList assert_subset
#' @importFrom yaml read_yaml
readDatabaseSettings <- function(path_databases_settings_yalm, database_name) {

  #
  # Check parameters
  #

  checkmate::assertFileExists(path_databases_settings_yalm, extension = "yml")
  databases_settings <- yaml::read_yaml(path_databases_settings_yalm)
  checkmate::assertList(databases_settings, names = "named")
  checkmate::assert_subset(database_name, databases_settings |> names())

  #
  # function
  #
  database_settings <- databases_settings[[database_name]]

  if(database_settings$connectionDetails$dbms == "eunomia"){
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  }else{
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!database_settings$connectionDetails)
  }

  database_settings$connectionDetails <- connectionDetails

  # set tempEmulationSchema if in config
  if( "tempEmulationSchema" %in% names(database_settings$schemas)){
    options(sqlRenderTempEmulationSchema = database_settings$schemas$tempEmulationSchema)
  }


  # add cohort_names_table_name
  database_settings$tables$cohort_names_table <- paste0(database_settings$tables$cohort_table, "_names")


  return(database_settings)
}




#' checkDatabaseSettings
#'
#' @param database_settings list with database settings
#'
#' @return tibble with check resutls
#'
#' @importFrom DatabaseConnector createConnectionDetails connect dbGetQuery disconnect
#' @importFrom SqlRender translate
#' @importFrom dplyr add_row
#' @importFrom tibble tibble
checkDatabaseSettings <- function(database_settings) {

  #
  # Check parameters
  #

  checkmate::assertList(database_settings, names = "named")
  # TODO: check all parameters

  #
  # function
  #
  database_settings_checks <- tibble::tibble(
    step = as.character(NA),
    error = as.logical(NA),
    message = as.character(NA)
  ) |>  dplyr::filter(FALSE)


  # Check connection
  e <- tryCatch({
    connection <- DatabaseConnector::connect(database_settings$connectionDetails)
    DatabaseConnector::disconnect(connection)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "Connection details",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("Connected to database",database_settings$connection$dbms ))
    )

  # set sqlRenderTempEmulationSchema if given
  if( "sqlRenderTempEmulationSchema" %in% names(database_settings)){
    options(sqlRenderTempEmulationSchema = database_settings$sqlRenderTempEmulationSchema)
    database_settings_checks <-  database_settings_checks |>
      dplyr::add_row(
        step = "sqlRenderTempEmulationSchema",
        error = FALSE,
        message = paste("Set sqlRenderTempEmulationSchema to ", database_settings$sqlRenderTempEmulationSchema)
      )
  }


  ## Check schemas
  e <- tryCatch({
    connection <- DatabaseConnector::connect(database_settings$connectionDetails)
    cdm_source <- dplyr::tbl(connection, DatabaseConnector::inDatabaseSchema(database_settings$schemas$CDM, "cdm_source")) |>
      dplyr::collect() |>
      dplyr::slice(1)
    DatabaseConnector::disconnect(connection)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "CDM schema",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("CDM schema:", cdm_source$cdm_source_name, "version:", cdm_source$cdm_version ))
    )

  # Check tables
  cohort_table_created <- FALSE
  e <- tryCatch({
    cohort_table_created <- createCohortTables(database_settings)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "Workbench cohort table",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste(ifelse(cohort_table_created, "Workbench cohort table created", "Existing workbench cohort table loaded"), database_settings$tables$cohort_scratch))
    )


  # Check webAPI

  ## check URL
  e <- tryCatch({
    CdmSources <- ROhdsiWebApi::getCdmSources(database_settings$webApi$url)
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "Connection to webAPI",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("conected to webAPI", database_settings$webApi$url))
    )

  ## check sourceKey
  e <- tryCatch({
    CdmSources <- ROhdsiWebApi::getCdmSources(database_settings$webApi$url)
    if(!(database_settings$webApi$sourceKey %in% CdmSources$sourceKey)){
      stop("Invalid sourceKey")
    }
  }, error=function(cond){ return(cond$message)})

  database_settings_checks <-  database_settings_checks |>
    dplyr::add_row(
      step = "SourceKey",
      error = is.character(e),
      message = ifelse(is.character(e), as.character(e), paste("conected to webAPI's SourceKey", CdmSources$sourceKey))
    )



  return(database_settings_checks)


}
