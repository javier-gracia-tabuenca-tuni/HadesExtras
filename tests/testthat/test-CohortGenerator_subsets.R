
#
# createMatchingSubset
#
test_that("Matching subset naming and instantitation", {
  matchingSubsetNamed <- createMatchingSubset(
    matchToCohortId = 11,
    matchSex = TRUE,
    matchBirthYear = TRUE,
    matchRatio = 10,
    startWindowAsTarget = TRUE,
    endWindowAsTarget = TRUE
  )
  expectedName <- "Match to cohort 11 by sex and birth year with ratio 1:10; cohort start date as matching cohort; cohort end date as matching cohort"
  expect_equal(expectedName, matchingSubsetNamed$name)

  matchingSubsetNamed$name <- "foo"
  expect_equal("foo", matchingSubsetNamed$name)

  matchingSubsetNamed <- createMatchingSubset(
    matchToCohortId = 110,
    matchSex = TRUE,
    matchBirthYear = FALSE,
    matchRatio = 100,
    startWindowAsTarget = FALSE,
    endWindowAsTarget = FALSE
  )
  expectedName <- "Match to cohort 110 by sex with ratio 1:100"
  expect_equal(expectedName, matchingSubsetNamed$name)
})





test_that("Matching Subset works for different parameters", {

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()

  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = CohortGenerator::getCohortTableNames("my_cohort")
  )

  # cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
  #   settingsFileName = "testdata/matching/Cohorts.csv",
  #   jsonFolder = "testdata/matching/cohorts",
  #   sqlFolder = "testdata/matching/sql/sql_server",
  #   cohortFileNameFormat = "%s",
  #   cohortFileNameValue = c("cohortName"),
  #   packageName = "HadesExtras",
  #   verbose = FALSE
  # )

  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "inst/testdata/matching/Cohorts.csv",
    jsonFolder = "inst/testdata/matching/cohorts",
    sqlFolder = "inst/testdata/matching/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  # cohort 10:
  # 1 M born in 1970
  # 1 F born in 1971
  #
  # cohort 20:
  # 10 M born in 1970
  # 10 F born in 1970
  # 10 F born in 1971
  # 10 F born in 1972

  # Match to sex only, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = FALSE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = getCohortTableNames("my_cohort"),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  CohortGenerator::getCohortCounts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTable = "my_cohort"
  )


})











