



CohortTableHandler <- R6::R6Class(
  classname = "CohortTableHandler",
  private = list(
    cohortDefinitionSet = NULL
  ),
  public = list(
    # connection parameters
    connectionHandler = NULL,
    cohortDatabaseSchema = NULL,
    cohortTableNames = NULL,
    connectionStatus = NULL,
    #`
    #` Initialize the CohortTableHandler object
    #` @param connectionDetails             A DatabaseConnector::connectionDetails  object
    #` @param vocabularyDatabaseSchema      A string with the name of the vocabulary database schema
    #` @param cdmDatabaseSchema             A string with the name of the CDM database schema
    #` @param cohortDatabaseSchema          A string with the name of the cohort database schema
    initialize = function(
      connectionHandler,
      cohortDatabaseSchema,
      cohortTableName
    ) {
      self$cdmConnectionHandler <- cdmConnectionHandler
      self$cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTableName)

      #
      connectionStatus <- logTibble_NewLog()

      errorMessage <- ""
      tryCatch({
        CohortGenerator::createCohortTables(
          connection = self$connectionHandler$getConnection(),
          cohortDatabaseSchema = self$cohortDatabaseSchema,
          cohortTableNames = self$cohortTableNames)
      }, error=function(error){ errorMessage <<- error$message})

      if(errorMessage != "" ){
        connectionStatus <- logTibble_ERROR(connectionStatus, "Create cohort tables", errorMessage)
      }else{
        connectionStatus <- logTibble_INFO(connectionStatus, "Create cohort tables", "Created cohort tables")
      }


      private$cohortDefinitionSet <- CohortGenerator::createEmptyCohortDefinitionSet()
    }


  )



)
