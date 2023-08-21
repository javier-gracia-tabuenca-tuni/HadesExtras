#' CohortTableHandler
#'
#' @description
#' Class for handling cohort tables in a CDM database.
#' Inherits from CDMdbHandler.
#'
#' @field cohortDatabaseSchema Name of the cohort database schema (read-only).
#' @field cohortTableNames Names of the cohort tables (read-only).
#' @field incrementalFolder Path to folder used by CohortGenerator in inclemetnal mode (read-only).
#' @field cohortDefinitionSet Table in cohortDefinitionSet with the current cohorts in the cohortTable (read-only).
#' @field cohortGeneratorResults Table with results from CohortGenerator_generateCohortSet with the current cohorts in the cohortTable(read-only).
#' @field cohortDemograpics Table with results from CohortGenerator_getCohortDemograpics with the current cohorts in the cohortTable(read-only).
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
    # database parameters
    .cohortDatabaseSchema = NULL,
    .cohortTableNames = NULL,
    .incrementalFolder = NULL,
    # Internal cohorts data
    .cohortDefinitionSet = NULL,
    .cohortGeneratorResults = NULL,
    .cohortDemograpics = NULL
  ),
  active = list(
    # Read-only parameters
    # database parameters
    cohortDatabaseSchema = function(){return(private$.cohortDatabaseSchema)},
    cohortTableNames = function(){return(private$.cohortTableNames)},
    incrementalFolder = function(){return(private$.incrementalFolder)},
    # Internal cohorts data
    cohortDefinitionSet = function(){return(private$.cohortDefinitionSet)},
    cohortGeneratorResults = function(){return(private$.cohortGeneratorResults)},
    cohortDemograpics = function(){return(private$.cohortDemograpics)}
  ),
  public = list(
    #' @description
    #' Initialize the CohortTableHandler object
    #'
    #' @param connectionHandler The connection handler object.
    #' @param databaseName A text id for the database the it connects to.
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

      private$.cohortDatabaseSchema <- cohortDatabaseSchema
      private$.cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTableName)
      private$.incrementalFolder <- file.path(tempdir(),stringr::str_remove_all(Sys.time(),"-|:|\\.|\\s"))

      private$.cohortDefinitionSet <- tibble::tibble(
        cohortId=0L,   cohortName="", sql="",        json="",
        .rows = 0 )

      private$.cohortGeneratorResults <- tibble::tibble(cohortId=0, buildInfo=list(), .rows = 0)
      private$.cohortDemograpics <- tibble::tibble(cohortId=0, .rows = 0)

      #self$loadConnection()
      # super$initialize is calling self$loadConnection(), self$loadConnection() is calling super$loadConnection()

      super$initialize(
        databaseName = databaseName,
        connectionHandler = connectionHandler,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema
      )
    },
    #' Finalize method
    #' @description
    #' Closes the connection if active.
    finalize = function() {

      print("deleted")

      CohortGenerator::dropCohortStatsTables(
        connection = self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        dropCohortTable = TRUE
      )
      unlink(private$.incrementalFolder, recursive = TRUE)

      super$finalize()
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
    #' If there is no cohort with the same cohortId it is added to the cohortDefinitionSet,
    #' If there is a cohort with the same cohortId, the cohort is updated in the cohortDefinitionSet
    #' CohortDefinitionSet is generated and demographics is updated for only the cohorts that have changed
    #'
    #' @param cohortDefinitionSet The cohort definition set to add.
    insertOrUpdateCohorts = function(cohortDefinitionSet) {
      #
      # Check parameters
      #
      #if(!CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet)){
      # TEMP not working
      # stop("Provided table is not of cohortDefinitionSet format")
      #}

      cohortIdsExists <- intersect( private$.cohortDefinitionSet$cohortId,  cohortDefinitionSet$cohortId  )
      if(length(cohortIdsExists)!=0){
        warning("Following cohort ids already exists on the cohort table and will be updated: ", paste(cohortIdsExists, collapse = ", "))
      }

      #
      # Function
      #
      # update existing cohorts in cohortDefinitionSet
      hasSubSets <- isTRUE(attr(private$.cohortDefinitionSet, "hasSubsetDefinitions")) | isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))
      cohortDefinitionSet <- dplyr::bind_rows(
        private$.cohortDefinitionSet |> dplyr::filter(!(cohortId %in% cohortIdsExists)),
        cohortDefinitionSet
      )|>
        dplyr::arrange(cohortId)
      attr(cohortDefinitionSet, "hasSubsetDefinitions") <- hasSubSets

      # generate cohorts in incremental mode
      cohortGeneratorResults <- CohortGenerator_generateCohortSet(
        connection= self$connectionHandler$getConnection(),
        cdmDatabaseSchema = self$cdmDatabaseSchema,
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        cohortDefinitionSet = cohortDefinitionSet ,
        incremental = TRUE,
        incrementalFolder = private$.incrementalFolder
      )|>
        dplyr::arrange(cohortId)

      # Update cohortDemograpics
      haveChangeCohortIds <- cohortGeneratorResults |>
        dplyr::filter(generationStatus == "COMPLETE") |>
        dplyr::pull(cohortId)

      cohortDemograpicsToUpdate <- tibble::tibble(cohortId=0, .rows = 0)
      if(length(haveChangeCohortIds)!=0){
        cohortDemograpicsToUpdate <- CohortGenerator_getCohortDemograpics(
          connection= self$connectionHandler$getConnection(),
          cdmDatabaseSchema = self$cdmDatabaseSchema,
          vocabularyDatabaseSchema = self$vocabularyDatabaseSchema,
          cohortDatabaseSchema = self$cohortDatabaseSchema,
          cohortTable = self$cohortTableNames$cohortTable,
          cohortIds = haveChangeCohortIds
        )
      }
      #
      cohortDemograpics <- dplyr::bind_rows(
        private$.cohortDemograpics |> dplyr::filter(!(cohortId %in% cohortDemograpicsToUpdate$cohortId)),
        cohortDemograpicsToUpdate
      ) |>
        dplyr::arrange(cohortId)

      # if no errors save
      private$.cohortDefinitionSet <- cohortDefinitionSet
      private$.cohortGeneratorResults <- cohortGeneratorResults
      private$.cohortDemograpics <- cohortDemograpics

    },
    #'
    #' deleteCohorts
    #' @description
    #' Deletes cohorts from the cohort table.
    #'
    #' @param cohortIds The cohort ids to delete.
    deleteCohorts = function(cohortIds) {
      #check parameters
      cohortIdsNotExists <- setdiff(cohortIds, private$.cohortDefinitionSet$cohortId)
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

      private$.cohortDefinitionSet <- private$.cohortDefinitionSet |>
        dplyr::filter(cohortId != cohortIds)

      private$.cohortGeneratorResults <- private$.cohortGeneratorResults |>
        dplyr::filter(cohortId != cohortIds)

      private$.cohortDemograpics <- private$.cohortDemograpics |>
        dplyr::filter(cohortId != cohortIds)

    },
    #'
    #' getCohortCounts
    #' @description
    #' Retrieves cohort counts from the cohort table.
    #'
    #' @return A tibble containing the cohort counts with names.
    getCohortCounts = function() {
      cohortCountsWithNames <- private$.cohortDefinitionSet |> dplyr::select(cohortName, cohortId) |>
        dplyr::left_join(
          private$.cohortDemograpics |> dplyr::select(cohortId, cohortEntries, cohortSubjects),
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

      cohortsSummaryWithNames <- private$.cohortDefinitionSet |> dplyr::select(cohortName, cohortId) |>
        dplyr::mutate(
          databaseName = super$databaseName,
          shortName = paste0("C", cohortId)
        ) |>
        dplyr::left_join(
          private$.cohortDemograpics,
          by= "cohortId"
        ) |>
        dplyr::left_join(
          private$.cohortGeneratorResults |>  dplyr::select(cohortId, buildInfo),
          by= "cohortId"
        ) |>
        correctEmptyCohortsInCohortsSummary()

      return(cohortsSummaryWithNames)
    },
    #'
    #' getCohortIdAndNames
    #' @description
    #' Retrieves the cohort names.
    #'
    #' @return A vector with the name of the cohorts
    getCohortIdAndNames  = function(){
      return(private$.cohortDefinitionSet |> dplyr::select(cohortId, cohortName))
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





















