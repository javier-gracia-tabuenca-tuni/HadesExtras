
#' CDMConnectionHandler
#' @description
#' This class handles the connection and provides methods to interact with the CDM database.
#'
#' @field connectionHandler A connection handler object.
#' @field vocabularyDatabaseSchema A string with the name of the vocabulary database schema.
#' @field cdmDatabaseSchema A string with the name of the CDM database schema.
#' @field scracthDatabaseSchema A string with the name of a read-write schema.
#' @field tempEmulationSchema A string with the name of the temporary emulation schema.
#' @field vocabularyInfo Information about the vocabulary database.
#' @field CDMInfo Information about the CDM database.
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
    connectionStatus = NULL,
    #
    vocabularyInfo = NULL,
    CDMInfo = NULL,
    #
    vocabularyTbl = NULL,
    cdmTbl = NULL,
    #`
    #` @param connectionDetails             A DatabaseConnector::connectionDetails  object
    #` @param vocabularyDatabaseSchema      A string with the name of the vocabulary database schema
    #` @param cdmDatabaseSchema             A string with the name of the CDM database schema
    #` @param scracthDatabaseSchema         A string with the name of of a read write schema
    initialize = function(
    connectionHandler,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema
    ) {

      checkmate::assertClass(connectionHandler, "ConnectionHandler")
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)

      self$vocabularyDatabaseSchema <- vocabularyDatabaseSchema
      self$cdmDatabaseSchema <- cdmDatabaseSchema

      self$updateConnection()

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

    #' Update connection status
    #' @description
    #' Updates the connection status by checking the database connection, vocabulary database schema, and CDM database schema.
    updateConnection = function(){

      connectionStatus <- logTibble_NewLog()

      # Check vocabularyDatabaseSchema
      vocabularyTbl <- list()
      vocabularyInfo <- NULL
      errorMessage <- ""
      tryCatch({
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
          vocabularyTbl[[paste0("getTbl_",tableName)]] <- function(){self$connectionHandler$tbl(tableName, self$vocabularyDatabaseSchema)}
        }

        vocabularyInfo <- vocabularyTbl$vocabulary |>
          dplyr::filter(vocabulary_id == "None") |>
          dplyr::select(vocabulary_name, vocabulary_version) |>
          dplyr::collect( n = 1)
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_ERROR(connectionStatus, "vocabularyDatabaseSchema connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "vocabularyDatabaseSchema connection",
                                           "Connected to vocabulary:", vocabularyInfo$vocabulary_name,
                                           "Version: ", vocabularyInfo$vocabulary_version)
      }


      # Check cdmDatabaseSchema
      cdmTbl <- list()
      CDMInfo <-  NULL
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

        tablesInCdmDatabaseSchema <- DatabaseConnector::getTableNames(self$connectionHandler$getConnection(), self$cdmDatabaseSchema)

        vocabularyTablesInCdmDatabaseSchema <- intersect(cdmTableNames, tablesInCdmDatabaseSchema)

        for (tableName in vocabularyTablesInCdmDatabaseSchema) {
          CDMTbl[[paste0("getTbl_",tableName)]] <- function(){self$connectionHandler$tbl(tableName, self$cdmDatabaseSchema)}
        }

        CDMInfo <- cdmTbl$cdm_source |>
          dplyr::select(cdm_source_name, cdm_source_abbreviation,  cdm_version ) |>
          dplyr::collect( n = 1)
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_ERROR(connectionStatus, "cdmDatabaseSchema connection", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "cdmDatabaseSchema connection",
                                           "Connected to cdm:", CDMInfo$cdm_source_name, "Version: ", CDMInfo$ cdm_version )
      }

      # update status
      unlockBinding("vocabularyInfo", self)
      unlockBinding("CDMInfo", self)
      unlockBinding("connectionStatus", self)
      unlockBinding("vocabularyTbl", self)
      unlockBinding("cdmTbl", self)
      self$vocabularyInfo <- vocabularyInfo
      self$CDMInfo <- CDMInfo
      self$connectionStatus <- connectionStatus
      self$vocabularyTbl <- vocabularyTbl
      self$cdmTbl <- cdmTbl
      lockBinding("vocabularyInfo", self)
      lockBinding("CDMInfo", self)
      lockBinding("connectionStatus", self)
      lockBinding("vocabularyTbl", self)
      lockBinding("cdmTbl", self)
    }
  )
)


