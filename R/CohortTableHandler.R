



CohortTableHandler <- R6::R6Class(
  classname = "CohortTableHandler",
  private = list(
    # connection parameters
    connectionHandler = NULL,
    vocabularyDatabaseSchema = NULL,
    cdmDatabaseSchema = NULL,
    cohortDatabaseSchema = NULL,
    # cohort table parameters
    cohortTable = NULL,
    cohortTableSummary = NULL
  ),
  public = list(
    #`
    #` Initialize the CohortTableHandler object
    #` @param connectionDetails             A DatabaseConnector::connectionDetails  object
    #` @param vocabularyDatabaseSchema      A string with the name of the vocabulary database schema
    #` @param cdmDatabaseSchema             A string with the name of the CDM database schema
    #` @param cohortDatabaseSchema          A string with the name of the cohort database schema
    initialize = function(connectionDetails, vocabularyDatabaseSchema, cdmDatabaseSchema, cohortDatabaseSchema, cohortTable) {
      private$connectionHandler <- ResultModelManager::ConnectionHandler$new(connectionDetails)
      private$vocabularyDatabaseSchema <- vocabularyDatabaseSchema
      private$cdmDatabaseSchema <- cdmDatabaseSchema
      private$cohortDatabaseSchema <- cohortDatabaseSchema
      private$cohortTable <- cohortTable

      # create cohort table
      CohortGenerator::createCohortTables(
        connection = private$connectionHandler$getConnection(),
        cohortDatabaseSchema = private$cohortDatabaseSchema,
        cohortTableNames = CohortGenerator::getCohortTableNames(private$cohortTable))
    }

    #`
    #` Get the cohort table



  )



)
