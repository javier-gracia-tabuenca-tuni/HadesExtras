#' CDMdbHandler
#'
#' @description
#' Class for handling database connection and schema information for a CDM database
#'
#' @field databaseName           A text id for the database the it connects to (read-only).
#' @field connectionHandler           ConnectionHandler object for managing the database connection (read-only).
#' @field vocabularyDatabaseSchema    Name of the vocabulary database schema (read-only).
#' @field cdmDatabaseSchema           Name of the CDM database schema (read-only).
#' @field connectionStatusLog            Log tibble object for storing connection status information (read-only).
#' @field vocabularyInfo              Data frame containing information about the vocabulary database (read-only).
#' @field CDMInfo                     Data frame containing information about the CDM database (read-only).
#' @field getTblVocabularySchema               List of functions that create dbplyr table for the vocabulary tables (read-only).
#' @field getTblCDMSchema                      List of functions that create dbplyr table for the CDM tables (read-only).
#'
#' @importFrom R6 R6Class
#' @importFrom checkmate assertClass assertString
#' @importFrom dplyr filter select collect
#' @importFrom DBI dbIsValid
#' @importFrom DatabaseConnector connect disconnect getTableNames dropEmulatedTempTables
#'
#' @export
CDMdbHandler <- R6::R6Class(
  classname = "CDMdbHandler",
  private = list(
    .databaseName = NULL,
    # database parameters
    .connectionHandler = NULL,
    .vocabularyDatabaseSchema = NULL,
    .cdmDatabaseSchema = NULL,
    .connectionStatusLog = NULL,
    #
    .vocabularyInfo = NULL,
    .CDMInfo = NULL,
    #
    .getTblVocabularySchema = NULL,
    .getTblCDMSchema = NULL
  ),
  active = list(
    databaseName = function(){return(private$.databaseName)},
    # database parameters
    connectionHandler = function(){return(private$.connectionHandler)},
    vocabularyDatabaseSchema = function(){return(private$.vocabularyDatabaseSchema)},
    cdmDatabaseSchema = function(){return(private$.cdmDatabaseSchema)},
    connectionStatusLog = function(){return(private$.connectionStatusLog$logTibble |>
                                              dplyr::mutate(databaseName = private$.databaseName) |>
                                              dplyr::relocate(databaseName, .before = 1))},
    #
    vocabularyInfo = function(){return(private$.vocabularyInfo)},
    CDMInfo = function(){return(private$.CDMInfo)},
    #
    getTblVocabularySchema = function(){return(private$.getTblVocabularySchema)},
    getTblCDMSchema = function(){return(private$.getTblCDMSchema)}
  ),
  public = list(
    #'
    #' @param databaseName           A text id for the database the it connects to
    #' @param connectionHandler             A ConnectionHandler object
    #' @param cdmDatabaseSchema             Name of the CDM database schema
    #' @param vocabularyDatabaseSchema      (Optional) Name of the vocabulary database schema (default is cdmDatabaseSchema)
    initialize = function(
    databaseName,
    connectionHandler,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema) {
      checkmate::assertString(databaseName)
      checkmate::assertClass(connectionHandler, "ConnectionHandler")
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)

      private$.databaseName <- databaseName
      private$.connectionHandler <- connectionHandler
      private$.vocabularyDatabaseSchema <- vocabularyDatabaseSchema
      private$.cdmDatabaseSchema <- cdmDatabaseSchema

      self$loadConnection()
    },

    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {
      private$.connectionHandler$finalize()
    },

    #' Reload connection
    #' @description
    #' Updates the connection status by checking the database connection, vocabulary database schema, and CDM database schema.
    loadConnection = function() {
      connectionStatusLog <- LogTibble$new()

      # Check db connection
      errorMessage <- ""
      tryCatch(
        {
          private$.connectionHandler$initConnection()
        },
        error = function(error) {
          errorMessage <<- error$message
        },
        warning = function(warning){}
      )

      if (errorMessage != "" | !private$.connectionHandler$dbIsValid()) {
        connectionStatusLog$ERROR("Check database connection", errorMessage)
      } else {
        connectionStatusLog$INFO("Check database connection", "Valid connection")
      }

      # Check can create temp tables
      errorMessage <- ""
      tryCatch(
        {
          private$.connectionHandler$getConnection() |>
            tmp_dplyr_copy_to(cars, overwrite = TRUE)
          private$.connectionHandler$getConnection() |>
            DatabaseConnector::dropEmulatedTempTables()
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )

      if (errorMessage != "") {
        connectionStatusLog$WARNING("Check temp table creation", errorMessage)
      } else {
        connectionStatusLog$INFO("Check temp table creation", "can create temp tables")
      }

      # Check vocabularyDatabaseSchema and populates getTblVocabularySchema
      getTblVocabularySchema <- list()
      vocabularyInfo <- NULL
      errorMessage <- ""
      tryCatch(
        {
          # create dbplyr object for all tables in the vocabulary
          vocabularyTableNames <- c(
            "concept",
            "vocabulary",
            "domain",
            "concept_class",
            "concept_relationship",
            "relationship",
            "concept_synonym",
            "concept_ancestor",
            "source_to_concept_map",
            "drug_strength"
          )

          tablesInVocabularyDatabaseSchema <- DatabaseConnector::getTableNames(private$.connectionHandler$getConnection(), private$.vocabularyDatabaseSchema)

          vocabularyTablesInVocabularyDatabaseSchema <- intersect(vocabularyTableNames, tablesInVocabularyDatabaseSchema)

          for (tableName in vocabularyTablesInVocabularyDatabaseSchema) {
            text <- paste0('function() { private$.connectionHandler$tbl( "',tableName, '", "', private$.vocabularyDatabaseSchema,'")}')
            getTblVocabularySchema[[tableName]] <- eval(parse(text = text))
          }

          vocabularyInfo <- getTblVocabularySchema$vocabulary() |>
            dplyr::filter(vocabulary_id == "None") |>
            dplyr::select(vocabulary_name, vocabulary_version) |>
            dplyr::collect(n = 1)
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )

      if (errorMessage != "") {
        connectionStatusLog$ERROR("vocabularyDatabaseSchema connection", errorMessage)
      } else {
        connectionStatusLog$INFO(
          "vocabularyDatabaseSchema connection",
          "Connected to vocabulary:", vocabularyInfo$vocabulary_name,
          "Version: ", vocabularyInfo$vocabulary_version
        )
      }


      # Check cdmDatabaseSchema and populates getTblCDMSchema
      getTblCDMSchema <- list()
      CDMInfo <- NULL
      errorMessage <- ""
      tryCatch(
        {
          cdmTableNames <- c(
            "person",
            "observation_period",
            "visit_occurrence",
            "visit_detail",
            "condition_occurrence",
            "drug_exposure",
            "procedure_occurrence",
            "device_exposure",
            "measurement",
            "observation",
            "death",
            "note",
            "note_nlp",
            "specimen",
            "fact_relationship",
            "location",
            "care_site",
            "provider",
            "payer_plan_period",
            "cost",
            "drug_era",
            "dose_era",
            "condition_era",
            "episode",
            "episode_event",
            "metadata",
            "cdm_source"
          )

          tablesInCdmDatabaseSchema <- DatabaseConnector::getTableNames(private$.connectionHandler$getConnection(), private$.cdmDatabaseSchema)

          vocabularyTablesInCdmDatabaseSchema <- intersect(cdmTableNames, tablesInCdmDatabaseSchema)

          for (tableName in vocabularyTablesInCdmDatabaseSchema) {
            text <- paste0('function() { private$.connectionHandler$tbl( "',tableName, '", "', private$.cdmDatabaseSchema,'")}')
            getTblCDMSchema[[tableName]] <- eval(parse(text = text))
          }

          CDMInfo <- getTblCDMSchema$cdm_source() |>
            dplyr::select(cdm_source_name, cdm_source_abbreviation, cdm_version) |>
            dplyr::collect(n = 1)
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )

      if (errorMessage != "") {
        connectionStatusLog$ERROR("cdmDatabaseSchema connection", errorMessage)
      } else {
        connectionStatusLog$INFO(
          "cdmDatabaseSchema connection",
          "Connected to cdm:", CDMInfo$cdm_source_name, "Version: ", CDMInfo$ cdm_version
        )
      }


      # update status
      private$.vocabularyInfo <- vocabularyInfo
      private$.CDMInfo <- CDMInfo
      private$.connectionStatusLog <- connectionStatusLog
      private$.getTblVocabularySchema <- getTblVocabularySchema
      private$.getTblCDMSchema <- getTblCDMSchema
    }
  )
)


#' createCDMdbHandlerFromList
#'
#' A function to create a CDMdbHandler object from a list of configuration settings.
#'
#' @param config A list containing configuration settings for the CDMdbHandler.
#'   - databaseName: The name of the database.
#'   - connection: A list of connection details settings.
#'   - cdm: A list of CDM database schema settings.
#'   - cohortTable: The name of the cohort table.
#'
#' @return A CDMdbHandler object.
#'
#' @importFrom checkmate assertList assertSubset
#'
#' @export
createCDMdbHandlerFromList <- function(
    config
  ) {

  config |> checkmate::assertList()
  config |> names() |> checkmate::assertNames(must.include = c("databaseName", "connection", "cdm" ))

  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = config$connection$connectionDetailsSettings,
    tempEmulationSchema = config$connection$tempEmulationSchema,
    useBigrqueryUpload = config$connection$useBigrqueryUpload
  )

  CDMdb <- CDMdbHandler$new(
    databaseName = config$databaseName,
    connectionHandler = connectionHandler,
    cdmDatabaseSchema = config$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = config$cdm$vocabularyDatabaseSchema
  )

  return(CDMdb)

}
































