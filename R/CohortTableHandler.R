#' CohortTableHandler
#'
#' @description
#' Class for handling cohort tables in a CDM database.
#' Inherits from CDMdbHandler.
#'
#' @field cohortDatabaseSchema Name of the cohort database schema.
#' @field cohortTableNames Names of the cohort tables.
#'
#' @importFrom R6 R6Class
#' @importFrom checkmate assertClass assertString
#' @importFrom CohortGenerator createEmptyCohortDefinitionSet createCohortTables getCohortTableNames generateCohortSet getCohortCounts dropCohortStatsTables
#' @importFrom dplyr bind_rows filter left_join count collect nest_by mutate select
#' @importFrom stringr str_remove_all
#'
#' @export
#'
CohortTableHandler <- R6::R6Class(
  classname = "CohortTableHandler",
  inherit = CDMdbHandler,
  private = list(
    cohortDefinitionSet = NULL,
    cohortsSummary = NULL,
    incrementalFolder = NULL
  ),
  public = list(
    # database parameters
    cohortDatabaseSchema = NULL,
    cohortTableNames = NULL,
    #' @description
    #' Initialize the CohortTableHandler object
    #'
    #' @param connectionHandler The connection handler object.
    #' @param cdmDatabaseSchema Name of the CDM database schema.
    #' @param vocabularyDatabaseSchema Name of the vocabulary database schema. Default is the same as the CDM database schema.
    #' @param cohortDatabaseSchema Name of the cohort database schema.
    #' @param cohortTableName Name of the cohort table.
    initialize = function(connectionHandler,
                          databaseName,
                          cdmDatabaseSchema,
                          vocabularyDatabaseSchema = cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTableName) {
      checkmate::assertClass(connectionHandler, "ConnectionHandler")
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)
      checkmate::assertString(cohortDatabaseSchema)
      checkmate::assertString(cohortTableName)

      super$initialize(
        databaseName = databaseName,
        connectionHandler = connectionHandler,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema
      )

      self$cohortDatabaseSchema <- cohortDatabaseSchema
      self$cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTableName)

      private$cohortDefinitionSet <- tibble::tibble(
        cohortId=0,   cohortName="", sql="",        json="",
        sourceCohortId = 0, cohortDescription = "",
        .rows = 0 )

      private$cohortsSummary <- createEmptyCohortsSummary() |>
        dplyr::select(
          cohortId, cohortEntries, cohortSubjects,
          histogram_cohort_start_year, histogram_cohort_end_year, count_sex
        )

      private$incrementalFolder <- file.path(tempdir(),stringr::str_remove_all(Sys.time(),"-|:|\\.|\\s"))

      self$loadConnection()
    },
    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {
      CohortGenerator::dropCohortStatsTables(
        connection = self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        dropCohortTable = TRUE
      )
      super$finalize()

      unlink(private$incrementalFolder, recursive = TRUE)
    },
    #'
    #' loadConnection
    #' @description
    #' Reloads the connection with the initial setting and updates connection status
    loadConnection = function() {
      super$loadConnection()

      # Check cohort database schema
      errorMessage <- ""
      tryCatch(
        {
          CohortGenerator::createCohortTables(
            connection = self$connectionHandler$getConnection(),
            cohortDatabaseSchema = self$cohortDatabaseSchema,
            cohortTableNames = self$cohortTableNames
          )
        },
        error = function(error) {
          errorMessage <<- error$message
        }
      )

      if (errorMessage != "") {
        self$connectionStatusLog$ERROR("Create cohort tables", errorMessage)
      } else {
        self$connectionStatusLog$INFO("Create cohort tables", "Created cohort tables")
      }
    },
    #'
    #' insertOrUpdateCohorts
    #' @description
    #' If there is no cohort with the same name it is added and generated with a new cohort_id, given cohort_id is saved as source_cohort_id,
    #' If there is a cohort with the same name, the current cohort_id is kept and source_cohort_id is updated
    #'
    #' @param cohortDefinitionSet The cohort definition set to add.
    insertOrUpdateCohorts = function(cohortDefinitionSet) {
      #
      # Check parameters
      #
      if(!CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet)){
        stop("Provided table is not of cohortDefinitionSet format")
      }

      cohortNamesExists <- intersect( private$cohortDefinitionSet$cohortName,  cohortDefinitionSet$cohortName  )
      if(length(cohortNamesExists)!=0){
        warning("Following cohort names already exists on the cohort table and will be updated: ", paste(cohortNamesExists, collapse = ", "))
      }

      #
      # Function
      #

      # change cohortId to not overlap with existing cohorts
      # maxCohortId <- ifelse(nrow(private$cohortDefinitionSet)==0, 0, max(private$cohortDefinitionSet$cohortId))
      # cohortDefinitionSet <- cohortDefinitionSet |>
      #   dplyr::rename(sourceCohortId = cohortId) |>
      #   dplyr::left_join(
      #     private$cohortDefinitionSet |> dplyr::select(cohortId, cohortName),
      #     by = "cohortName"
      #   ) |>
      #   dplyr::mutate(
      #     cohortId = dplyr::if_else(is.na(cohortId), sourceCohortId + maxCohortId, cohortId)
      #   )

      # update existing cohorts
      hasSubSets <- isTRUE(attr(private$cohortDefinitionSet, "hasSubsetDefinitions")) | isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))
      cohortDefinitionSet <- dplyr::bind_rows(
        private$cohortDefinitionSet |> dplyr::filter(!(cohortName %in% cohortNamesExists)),
        cohortDefinitionSet
      )
      attr(cohortDefinitionSet, "hasSubsetDefinitions") <- hasSubSets


      CohortGenerator::generateCohortSet(
        connection= self$connectionHandler$getConnection(),
        cdmDatabaseSchema = self$cdmDatabaseSchema,
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        cohortDefinitionSet = cohortDefinitionSet ,
        incremental = TRUE,
        incrementalFolder = private$incrementalFolder
      )

      # Update cohortsSummary
      # TODO: at the moment runs for all cohorts every time a cohort is add,
      #       it has to change to generate only the added cohorts
      cohortDemograpics <- CohortGenerator_getCohortDemograpics(
        connection= self$connectionHandler$getConnection(),
        cdmDatabaseSchema = self$cdmDatabaseSchema,
        vocabularyDatabaseSchema = self$vocabularyDatabaseSchema,
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTable = self$cohortTableNames$cohortTable
      )

      # correct if n patients in cohort is 0
      cohortsSummary <- correctEmptyCohortsInCohortsSummary(cohortsSummary)

      # if no errors save to private$cohortDefinitionSet
      private$cohortDefinitionSet <- cohortDefinitionSet
      private$cohortsSummary <- cohortsSummary |>
        dplyr::mutate(
          cohortId = as.integer(cohortId),
          cohortEntries = as.integer(cohortEntries),
          cohortSubjects = as.integer(cohortSubjects)
        )

    },
    #'
    #' deleteCohorts
    #' @description
    #' Deletes cohorts from the cohort table.
    #'
    #' @param cohortIds The cohort ids to delete.
    deleteCohorts = function(cohortIds) {
      #check parameters
      cohortIdsNotExists <- setdiff(cohortIds, private$cohortDefinitionSet$cohortId)
      if(length(cohortIdsNotExists)!=0){
        stop("Following cohort ids dont exists on the cohort table: ", paste(cohortIdsNotExists, collapse = ", "))
      }

      # function
      CohortGenerator_deleteCohortFromCohortTable(
        connection= self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        cohortIds = cohortIds
      )

      private$cohortDefinitionSet <- private$cohortDefinitionSet |>
        dplyr::filter(cohortId != cohortIds)

      private$cohortsSummary <- private$cohortsSummary |>
        dplyr::filter(cohortId != cohortIds)

    },
    #'
    #' getCohortCounts
    #' @description
    #' Retrieves cohort counts from the cohort table.
    #'
    #' @return A tibble containing the cohort counts with names.
    getCohortCounts = function() {
      cohortCountsWithNames <- dplyr::left_join(
        private$cohortDefinitionSet |> dplyr::select(cohortName, cohortId),
        private$cohortsSummary |> dplyr::select(cohortId, cohortEntries, cohortSubjects),
        by= "cohortId"
      )

      return(cohortCountsWithNames)
    },
    #'
    #' getCohortsSummary
    #' @description
    #' Retrieves the summary of cohorts including cohort start and end year histograms and sex counts.
    #'
    #' @return A tibble containing cohort summary.
    getCohortsSummary  = function(){
      cohortsSummaryWithNames <- dplyr::left_join(
        private$cohortDefinitionSet |> dplyr::select(cohortName, cohortId, sourceCohortId, cohortDescription),
        private$cohortsSummary,
        by= "cohortId"
      )

      return(cohortsSummaryWithNames)
    },
    #'
    #' getCohortNames
    #' @description
    #' Retrieves the cohort names.
    #'
    #' @return A vector with the name of the cohorts
    getCohortNames  = function(){
      return(private$cohortDefinitionSet |> dplyr::pull(cohortName))
    }

  )
)


#' createCohortTableHandlerFromList
#'
#' A function to create a CohortTableHandler object from a list of configuration settings.
#'
#' @param config A list containing configuration settings for the CohortTableHandler.
#'   - databaseName: The name of the database.
#'   - connection: A list of connection details settings.
#'   - cdm: A list of CDM database schema settings.
#'   - cohortTable: A list of cohort table settings.
#'
#' @return A CohortTableHandler object.
#'
#' @importFrom checkmate assertList assertSubset
#'
#' @export
createCohortTableHandlerFromList <- function(
    config
) {

  config |> checkmate::assertList()
  config |> names() |> checkmate::assertSubset(c("databaseName", "connection", "cdm", "cohortTable" ))

  connectionHandler <- ResultModelManager_createConnectionHandler(
    connectionDetailsSettings = config$connection$connectionDetailsSettings,
    tempEmulationSchema = config$connection$tempEmulationSchema,
    useBigrqueryUpload = config$connection$useBigrqueryUpload
  )
  cohortTableHandler <- CohortTableHandler$new(
    connectionHandler = connectionHandler,
    databaseName = config$databaseName,
    cdmDatabaseSchema = config$cdm$cdmDatabaseSchema,
    vocabularyDatabaseSchema = config$cdm$vocabularyDatabaseSchema,
    cohortDatabaseSchema = config$cohortTable$cohortDatabaseSchema,
    cohortTableName = config$cohortTable$cohortTableName
  )

  return(cohortTableHandler)

}





















