
#
# createMatchingSubset
#
test_that("Matching subset naming and instantitation", {
  matchingSubsetNamed <- createMatchingSubset(
    name = NULL,
    matchToCohortId = 11,
    matchRatio = 10,
    matchSex = TRUE,
    matchBirthYear = TRUE,
    matchCohortStartDateWithInDuration = FALSE,
    newCohortStartDate = "asMatch",
    newCohortEndDate = "asMatch"
  )
  expectedName <- "Match to cohort 11 by sex and birth year with ratio 1:10; cohort start date as in matched subject; cohort end date as in matched subject"
  expect_equal(expectedName, matchingSubsetNamed$name)

  matchingSubsetNamed$name <- "foo"
  expect_equal("foo", matchingSubsetNamed$name)

  matchingSubsetNamed <- createMatchingSubset(
    matchToCohortId = 110,
    matchSex = TRUE,
    matchBirthYear = FALSE,
    matchRatio = 100,
    newCohortStartDate = "keep",
    newCohortEndDate = "keep"
  )
  expectedName <- "Match to cohort 110 by sex with ratio 1:100"
  expect_equal(expectedName, matchingSubsetNamed$name)
})



test_that("Matching Subset works", {

  connection <- helper_createNewConnection()
  #on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName)
  )


  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/matching/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/matching/cohorts"),
    sqlFolder = here::here("inst/testdata/matching/sql/sql_server"),
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
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = TRUE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName
  )

  checkmate::expect_tibble(cohortDemographics)

})



test_that("Matching Subset works for different parameters", {


  testthat::skip_if(testSelectedConfiguration$connection$connectionDetailsSettings$dbms!="eunomia")

  connection <- helper_createNewConnection()
  #on.exit({DatabaseConnector::dropEmulatedTempTables(connection); DatabaseConnector::disconnect(connection)})

  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName)
  )


  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/matching/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/matching/cohorts"),
    sqlFolder = here::here("inst/testdata/matching/sql/sql_server"),
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
        matchSex = TRUE,
        matchBirthYear = FALSE,
        matchCohortStartDateWithInDuration = FALSE,
        newCohortStartDate = "keep",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName
  )

  cohortDemographics$sexCounts[[3]]$n[[1]] |> expect_equal(20) # female
  cohortDemographics$sexCounts[[3]]$n[[2]] |> expect_equal(10) # male, there is only 10 in controls


  # Match to sex and bday, match ratio 20
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = TRUE,
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
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName
  )

  cohortDemographics$sexCounts[[3]]$n[[1]] |> expect_equal(10) # female, there is only 10
  cohortDemographics$sexCounts[[3]]$n[[2]] |> expect_equal(10) # male, there is only 10 in controls
  cohortDemographics$histogramBirthYear[[3]]$n[[1]] |> expect_equal(10) # 1970, there is only 10 in controls
  cohortDemographics$histogramBirthYear[[3]]$n[[2]] |> expect_equal(10) # 1971, there is only 10 in controls


  # Match to sex and bday and start day with in observation, keep startday
  subsetDef <- CohortGenerator::createCohortSubsetDefinition(
    name = "",
    definitionId = 300,
    subsetOperators = list(
      createMatchingSubset(
        matchToCohortId = 10,
        matchRatio = 20,
        matchSex = TRUE,
        matchBirthYear = TRUE,
        matchCohortStartDateWithInDuration = TRUE,
        newCohortStartDate = "asMatch",
        newCohortEndDate = "keep"
      )
    )
  )

  cohortDefinitionSetWithSubsetDef <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(subsetDef, targetCohortIds = 20)

  generatedCohorts <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTableNames = CohortGenerator::getCohortTableNames(testSelectedConfiguration$cohortTable$cohortTableName),
    cohortDefinitionSet = cohortDefinitionSetWithSubsetDef,
    incremental = FALSE
  )

  cohortDemographics <- CohortGenerator_getCohortDemograpics(
    connection = connection,
    cdmDatabaseSchema = testSelectedConfiguration$cdm$cdmDatabaseSchema,
    cohortDatabaseSchema = testSelectedConfiguration$cohortTable$cohortDatabaseSchema,
    cohortTable = testSelectedConfiguration$cohortTable$cohortTableName
  )

  cohortDemographics$sexCounts[[3]]$n[[1]] |> expect_equal(10) # female, half
  cohortDemographics$sexCounts[[3]]$n[[2]] |> expect_equal(6) # male, half
  cohortDemographics$histogramBirthYear[[3]]$n[[1]] |> expect_equal(6) # 1970, there is only 10 in controls
  cohortDemographics$histogramBirthYear[[3]]$n[[2]] |> expect_equal(10) # 1971, there is only 10 in controls

  cohortDemographics$histogramBirthYear[[1]]$year |> expect_equal(cohortDemographics$histogramBirthYear[[3]]$year)


})











