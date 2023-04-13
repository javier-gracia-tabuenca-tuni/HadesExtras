#' Title
#'
#' @param path_databases_settings_json
#'
#' @return tibble with
#' - database_name
#' - database_settings
#' - connection_test_results
#' @export
#'
#' @examples
readDatabasesSettings <- function(path_databases_settings_yalm) {

  checkmate::assertFileExists(path_databases_settings_yalm, extension = "yml")
  databases_settings <- yaml::read_yaml(path_databases_settings_yalm)
  checkmate::assertList(databases_settings, names = "named")


  databases_settings_tibble <- tibble::tibble(
    database_name = databases_settings |> names(),
    database_settings = databases_settings
  ) |>
    dplyr::mutate(purrr::map_dfr(database_settings, .checkDatabaseSettings))


  databases_settings_tibble

}




.checkDatabaseSettings <- function(database_settings) {

  # TODO check all parameters are valid

  #
  # connection
  #

  # check connection settings are accepted by createConnectionDetails
  e <- tryCatch({
    database_settings$connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!database_settings$connection)
  }, error=function(cond){ return(cond$message)})

  if(is.character(e)){
    return(tibble::tibble(
      database_settings = list(database_settings),
      connection_errors = paste("Error in connection settings: ", e)
    ))
  }

  #
  # schemas
  #

  # check schemas are valid
  e <- tryCatch({
    connection <- DatabaseConnector::connect(database_settings$connectionDetails)
    # parameterized sql
    sql <- " SELECT * FROM @cdm_schema.cdm_source LIMIT 10"

    # Render parameterized sql
    sql <- SqlRender::render(
      sql,
      cdm_schema = database_settings$schemas$CDM
    )

    # translate sql to target database
    sql <- SqlRender::translate(sql, targetDialect = database_settings$connectionDetails$dbms)

    # execute sql
    DatabaseConnector::dbGetQuery(connection, sql)
    DatabaseConnector::disconnect(connection)
  }, error=function(cond){ return(cond$message)})

  if(is.character(e)){
    return(tibble::tibble(
      database_settings = list(database_settings),
      connection_errors = paste("Could not connect to CDM schema Error: ", e)
    ))
  }


  #
  # webapi
  #

  # Test connection to webAPI
  e <- tryCatch({
    CdmSources <- ROhdsiWebApi::getCdmSources(database_settings$webApi$url)
  }, error=function(cond){ return(cond$message)})

  if(is.character(e)){
    return(tibble::tibble(
      database_settings = list(database_settings),
      connection_errors = paste("Could not connect to webAPI Error: ", e)
    ))
  }

  # test CDM_source_key exists
  if(!(database_settings$webApi$sourceKey %in% CdmSources$sourceKey)){
    return(tibble::tibble(
      database_settings = list(database_settings),
      connection_errors = paste("Invalid sourceKey: ", database_settings$webApi$sourceKey)
    ))
  }



  return(tibble::tibble(
    database_settings = list(database_settings),
    connection_errors = ""
  ))


}
