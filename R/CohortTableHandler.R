#'
#' CohortTableHandler
#'
#' @description
#' Class for handling cohort tables in a cdm database.
#' Inherits from CDMdbHandler.
#'
#' @inheritParams CDMdbHandler
#'
#' @field cohortDatabaseSchema      Name of the cohort database schema
#' @field cohortTableNames          Names of the cohort tables
#'
#' @importFrom R6 R6Class
#' @importFrom checkmate assertClass assertString
#' @importFrom CohortGenerator createEmptyCohortDefinitionSet createCohortTables getCohortTableNames
#'
#' @export CohortTableHandler
CohortTableHandler <- R6::R6Class(
  classname = "CohortTableHandler",
  inherit = CDMdbHandler,
  private = list(
    cohortDefinitionSet = NULL,
    incrementalFolder = NULL
  ),
  public = list(
    # database parameters
    cohortDatabaseSchema = NULL,
    cohortTableNames = NULL,
    #'
    #' Initialize the CohortTableHandler object
    #'
    #' @inheritParams CDMdbHandler
    #' @param cohortDatabaseSchema     Name of the cohort database schema
    #' @param cohortTableName          Name of the cohort table
    initialize = function(connectionHandler,
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
        connectionHandler = connectionHandler,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema
      )

      self$cohortDatabaseSchema <- cohortDatabaseSchema
      self$cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTableName)

      private$cohortDefinitionSet <- tibble::tibble(
        cohortId=0,   cohortName="", sql="",        json="",
        .rows = 0 )
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
    #' getCohortCounts
    #' @description
    #'
    getCohortCounts = function() {
      cohortCounts <- CohortGenerator::getCohortCounts(
        connection= self$connectionHandler$getConnection(),
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTable = self$cohortTableNames$cohortTable
      )

      cohortCountsWithNames <- dplyr::left_join(
        private$cohortDefinitionSet |> dplyr::select(cohortName, cohortId),
        cohortCounts,
        by= "cohortId"
      )

      return(cohortCountsWithNames)
    },
    #'
    #' addCohort
    #' @description
    #'
    addCohorts = function(cohortDefinitionSet) {
      #check parameters
      if(!CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet)){
        stop("Provided table is not of cohortDefinitionSet format")
      }

      cohortIdsExists <- intersect( private$cohortDefinitionSet$cohortId, cohortDefinitionSet$cohortId)
      if(length(cohortIdsExists)!=0){
        stop("Following cohort ids already exists on the cohort table: ", paste(cohortIdsExists, collapse = ", "))
      }

      cohortNamesExists <- intersect( private$cohortDefinitionSet$cohortName,  cohortDefinitionSet$cohortName  )
      if(length(cohortNamesExists)!=0){
        stop("Following cohort names already exists on the cohort table: ", paste(cohortNamesExists, collapse = ", "))
      }

      # function
      cohortDefinitionSet <- dplyr::bind_rows(
        private$cohortDefinitionSet,
        cohortDefinitionSet
      )

      CohortGenerator::generateCohortSet(
        connection= self$connectionHandler$getConnection(),
        cdmDatabaseSchema = self$cdmDatabaseSchema,
        cohortDatabaseSchema = self$cohortDatabaseSchema,
        cohortTableNames = self$cohortTableNames,
        cohortDefinitionSet = cohortDefinitionSet ,
        incremental = TRUE,
        incrementalFolder = private$incrementalFolder
      )

      # if no errors save to private$cohortDefinitionSet
      private$cohortDefinitionSet <- cohortDefinitionSet

    }
  )
)








