
#
# CohortGenerator_deleteCohortFromCohortTable
#
test_that("CohortGenerator_deleteCohortFromCohortTable deletes a cohort", {

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  cdmDatabaseSchema <- "main"
  cohortDatabaseSchema <- "main"
  cohortTable <- "cohort"
  Eunomia::createCohorts(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  )

  CohortGenerator_deleteCohortFromCohortTable(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable),
    cohortIds = c(1L,2L)
  )

  codeCounts <- CohortGenerator::getCohortCounts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  )

  codeCounts$cohortId |> expect_equal(c(3L,4L))
})




