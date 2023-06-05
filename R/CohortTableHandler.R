



CohortTableHandler <- R6::R6Class(
  classname = "CohortTableHandler",
  private = list(
    # connection parameters
    cdmConnectionHandler = NULL,
    # cohort table parameters
    cohortTableName = NULL,
    cohortDefinitionSet = NULL,
    cohortTableSummary = NULL
  ),
  public = list(
    #`
    #` Initialize the CohortTableHandler object
    #` @param connectionDetails             A DatabaseConnector::connectionDetails  object
    #` @param vocabularyDatabaseSchema      A string with the name of the vocabulary database schema
    #` @param cdmDatabaseSchema             A string with the name of the CDM database schema
    #` @param cohortDatabaseSchema          A string with the name of the cohort database schema
    initialize = function(cdmConnectionHandler, cohortTableName) {
      private$cdmConnectionHandler <- cdmConnectionHandler
      private$cohortTableName <- cohortTableName

      # create cohort table
      CohortGenerator::createCohortTables(
        connection = private$cdmConnectionHandler$connectionHandler$getConnection(),
        cohortDatabaseSchema = private$cdmConnectionHandler$scracthDatabaseSchema,
        cohortTableNames = CohortGenerator::getCohortTableNames(private$cohortTableName))

      private$cohortDefinitionSet <- CohortGenerator::createEmptyCohortDefinitionSet()
    }






#
#
#     #`
#     #` Get cohortDefinitionSet
#     #`
#     #` @return A CohortDefinitionSet object
#     getCohortDefinitionSet = function() {
#       private$cohortDefinitionSet
#     }
#
#     #` Get cohortTablesummary
#     #`
#     #` @return A CohortTableSummary object
#     getCohortTableSummary = function() {
#       private$cohortTableSummary
#     }
#
#



  )



)
