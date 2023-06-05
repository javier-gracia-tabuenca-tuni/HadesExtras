
#' CDMConnectionHandler
#' @description
#' This class handles the connection and provides methods to interact with the CDM database.
#'
#' @field connectionHandler A connection handler object.
#' @field vocabularyDatabaseSchema A string with the name of the vocabulary database schema.
#' @field cdmDatabaseSchema A string with the name of the CDM database schema.
#' @field scracthDatabaseSchema A string with the name of a read-write schema.
#' @field tempEmulationSchema A string with the name of the temporary emulation schema.
#' @field vocabInfo Information about the vocabulary database.
#' @field cdmInfo Information about the CDM database.
#' @field connectionStatus The connection status.
#'
#' @importFrom ResultModelManager PooledConnectionHandler ConnectionHandler
#' @importFrom checkmate assertClass assertString assertLogical
#' @importFrom dplyr filter select collect
#'
#' @export
CDMConnectionHandler <- R6::R6Class(
  classname = "CDMConnectionHandler",
  public = list(
    # connection parameters
    connectionHandler = NULL,
    vocabularyDatabaseSchema = NULL,
    cdmDatabaseSchema = NULL,
    scracthDatabaseSchema = NULL,
    tempEmulationSchema = NULL,
    #
    vocabInfo = NULL,
    cdmInfo = NULL,
    connectionStatus = NULL,
    #
    vocabularyTbl = NULL,
    cdmTbl = NULL,
    #`
    #` @param connectionDetails             A DatabaseConnector::connectionDetails  object
    #` @param vocabularyDatabaseSchema      A string with the name of the vocabulary database schema
    #` @param cdmDatabaseSchema             A string with the name of the CDM database schema
    #` @param scracthDatabaseSchema         A string with the name of of a read write schema
    initialize = function(
    connectionDetails = NULL,
    cdmDatabaseSchema = NULL,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    scracthDatabaseSchema = NULL,
    tempEmulationSchema = scracthDatabaseSchema,
    usePooledConnection = FALSE,
    ...
    ) {

      checkmate::assertClass(connectionDetails, "ConnectionDetails", null.ok = TRUE)
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)
      checkmate::assertString(scracthDatabaseSchema)
      checkmate::assertString(tempEmulationSchema)
      checkmate::assertLogical(usePooledConnection)



      if (usePooledConnection) {
        stop("not implemented")
        self$connectionHandler <- ResultModelManager::PooledConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
      } else {
        self$connectionHandler <- tmp_ConnectionHandler$new(connectionDetails, loadConnection = FALSE, ...)
      }

      self$vocabularyDatabaseSchema <- vocabularyDatabaseSchema
      self$cdmDatabaseSchema <- cdmDatabaseSchema
      self$scracthDatabaseSchema <- scracthDatabaseSchema
      self$tempEmulationSchema <- tempEmulationSchema

      options(sqlRenderTempEmulationSchema = scracthDatabaseSchema)

      self$updateConnectionStatus()


      lockBinding("connectionHandler", self)
      lockBinding("vocabularyDatabaseSchema", self)
      lockBinding("cdmDatabaseSchema", self)
      lockBinding("scracthDatabaseSchema", self)
      lockBinding("tempEmulationSchema", self)
      lockBinding("vocabularyTbl", self)

    },

    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {
      self$connectionHandler$finalize()
    },

    #' Update connection status
    #' @description
    #' Updates the connection status by checking the database connection, vocabulary database schema, and CDM database schema.
    updateConnectionStatus = function(){

      connectionStatus <- logTibble_NewLog()

      # Check connection
      errorMessage <- ""
      tryCatch({
        self$connectionHandler$initConnection()
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" | !self$connectionHandler$dbIsValid()){
        connectionStatus <- logTibble_ERROR(connectionStatus, "Database Connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "Database Connection", "Valid connection")
      }

      # Check vocabularyDatabaseSchema
      vocabularyTbl <- list()
      vocabInfo <- NULL
      errorMessage <- ""
      tryCatch({
        # create dbplyr object for all tables in the vocabulary
        vocabTableNames <- c(
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

        for (tableName in vocabTableNames) {
          try( vocabularyTbl[[tableName]] <- self$connectionHandler$tbl(tableName, self$vocabularyDatabaseSchema) )
        }

        vocabInfo <- vocabularyTbl$vocabulary |>
          dplyr::filter(vocabulary_id == "None") |>
          dplyr::select(vocabulary_name, vocabulary_version) |>
          dplyr::collect( n = 1)
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_ERROR(connectionStatus, "vocabularyDatabaseSchema connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "vocabularyDatabaseSchema connection",
                                           "Connected to vocabulary:", vocabInfo$vocabulary_name,
                                           "Version: ", vocabInfo$vocabulary_version)
      }


      # Check cdmDatabaseSchema
      cdmTbl <- list()
      cdmInfo <-  NULL
      errorMessage <- ""
      tryCatch({

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

        for (tableName in cdmTableNames) {
          try( cdmTbl[[tableName]] <- self$connectionHandler$tbl(tableName, self$cdmDatabaseSchema), silent = T )
        }

        cdmInfo <- cdmTbl$cdm_source |>
          dplyr::select(cdm_source_name, cdm_source_abbreviation,  cdm_version ) |>
          dplyr::collect( n = 1)
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_ERROR(connectionStatus, "cdmDatabaseSchema connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "cdmDatabaseSchema connection",
                                           "Connected to cdm:", cdmInfo$cdm_source_name, "Version: ", cdmInfo$ cdm_version )
      }

      # TODO: Check scracthDatabaseSchema is writble
      errorMessage <- ""
      tryCatch({
        self$connectionHandler$executeSql(
          sql = "IF OBJECT_ID('@scracthDatabaseSchema.write_test', 'U') IS NOT NULL
                  DROP TABLE @scracthDatabaseSchema.write_test;
                  CREATE TABLE @scracthDatabaseSchema.write_test (test INT);
                  INSERT INTO @scracthDatabaseSchema.write_test (test)
                  VALUES (5);",
          scracthDatabaseSchema = self$scracthDatabaseSchema)


        self$connectionHandler$executeSql(
          sql =  "IF OBJECT_ID('@scracthDatabaseSchema.write_test', 'U') IS NOT NULL DROP TABLE @scracthDatabaseSchema.write_test;",
          scracthDatabaseSchema = self$scracthDatabaseSchema
        )
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_ERROR(connectionStatus, "scracthDatabaseSchema connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "scracthDatabaseSchema connection", "scracthDatabaseSchema is writable")
      }

      #       # update status
      unlockBinding("vocabInfo", self)
      unlockBinding("cdmInfo", self)
      unlockBinding("connectionStatus", self)
      unlockBinding("vocabularyTbl", self)
      unlockBinding("cdmTbl", self)
      self$vocabInfo <- vocabInfo
      self$cdmInfo <- cdmInfo
      self$connectionStatus <- connectionStatus
      self$vocabularyTbl <- vocabularyTbl
      self$cdmTbl <- cdmTbl
      lockBinding("vocabInfo", self)
      lockBinding("cdmInfo", self)
      lockBinding("connectionStatus", self)
      lockBinding("vocabularyTbl", self)
      lockBinding("cdmTbl", self)
    }
  )
)


