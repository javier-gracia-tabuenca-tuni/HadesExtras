#' CDMdbHandler
#'
#' @description
#' Class for handling database connection and schema information for a CDM database
#'
#' @field connectionHandler           ConnectionHandler object for managing the database connection
#' @field vocabularyDatabaseSchema    Name of the vocabulary database schema
#' @field cdmDatabaseSchema           Name of the CDM database schema
#' @field connectionStatusLog            Log tibble object for storing connection status information
#' @field vocabularyInfo              Data frame containing information about the vocabulary database
#' @field CDMInfo                     Data frame containing information about the CDM database
#' @field getTblVocabularySchema               List of functions that create dbplyr table for the vocabulary tables
#' @field getTblCDMSchema                      List of functions that create dbplyr table for the CDM tables
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
  public = list(
    databaseName = NULL,
    # database parameters
    connectionHandler = NULL,
    vocabularyDatabaseSchema = NULL,
    cdmDatabaseSchema = NULL,
    connectionStatusLog = NULL,
    #
    vocabularyInfo = NULL,
    CDMInfo = NULL,
    #
    getTblVocabularySchema = NULL,
    getTblCDMSchema = NULL,
    #'
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

      self$databaseName <- databaseName
      self$connectionHandler <- connectionHandler
      self$vocabularyDatabaseSchema <- vocabularyDatabaseSchema
      self$cdmDatabaseSchema <- cdmDatabaseSchema

      self$loadConnection()

      lockBinding("databaseName", self)
      lockBinding("connectionHandler", self)
      lockBinding("vocabularyDatabaseSchema", self)
      lockBinding("cdmDatabaseSchema", self)
    },

    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {
      self$connectionHandler$finalize()
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
          self$connectionHandler$initConnection()
        },
        error = function(error) {
          errorMessage <<- error$message
        },
        warning = function(warning){}
      )

      if (errorMessage != "" | !self$connectionHandler$dbIsValid()) {
        connectionStatusLog$ERROR("Check database connection", errorMessage)
      } else {
        connectionStatusLog$INFO("Check database connection", "Valid connection")
      }

      # Check can create temp tables
      errorMessage <- ""
      tryCatch(
        {
          self$connectionHandler$getConnection() |>
            tmp_dplyr_copy_to(cars, overwrite = TRUE)
          self$connectionHandler$getConnection() |>
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

          tablesInVocabularyDatabaseSchema <- DatabaseConnector::getTableNames(self$connectionHandler$getConnection(), self$vocabularyDatabaseSchema)

          vocabularyTablesInVocabularyDatabaseSchema <- intersect(vocabularyTableNames, tablesInVocabularyDatabaseSchema)

          for (tableName in vocabularyTablesInVocabularyDatabaseSchema) {
            text <- paste0('function() { self$connectionHandler$tbl( "',tableName, '", "', self$vocabularyDatabaseSchema,'")}')
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

          tablesInCdmDatabaseSchema <- DatabaseConnector::getTableNames(self$connectionHandler$getConnection(), self$cdmDatabaseSchema)

          vocabularyTablesInCdmDatabaseSchema <- intersect(cdmTableNames, tablesInCdmDatabaseSchema)

          for (tableName in vocabularyTablesInCdmDatabaseSchema) {
            text <- paste0('function() { self$connectionHandler$tbl( "',tableName, '", "', self$cdmDatabaseSchema,'")}')
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
      unlockBinding("vocabularyInfo", self)
      unlockBinding("CDMInfo", self)
      unlockBinding("connectionStatusLog", self)
      unlockBinding("getTblVocabularySchema", self)
      unlockBinding("getTblCDMSchema", self)
      self$vocabularyInfo <- vocabularyInfo
      self$CDMInfo <- CDMInfo
      self$connectionStatusLog <- connectionStatusLog
      self$getTblVocabularySchema <- getTblVocabularySchema
      self$getTblCDMSchema <- getTblCDMSchema
      lockBinding("vocabularyInfo", self)
      lockBinding("CDMInfo", self)
      lockBinding("connectionStatusLog", self)
      lockBinding("getTblVocabularySchema", self)
      lockBinding("getTblCDMSchema", self)
    },

    #' Get connection status
    #' @description
    #' gets tibble with database name and connection status.
    getConnectionStatus = function() {
      self$connectionStatusLog$logTibble |>
        dplyr::mutate(databaseName = self$databaseName) |>
        dplyr::relocate(databaseName, .before = 1)

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
  config |> names() |> checkmate::assertSubset(c("databaseName", "connection", "cdm" ))

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
































