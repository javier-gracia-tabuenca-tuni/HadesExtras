

test_that("rectable_cohortsSummary works", {

  cohortTableHandler <- helper_createNewCohortTableHandler()
  on.exit({rm(cohortTableHandler);gc()})

  # cohorts from eunomia
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = here::here("inst/testdata/asthma/Cohorts.csv"),
    jsonFolder = here::here("inst/testdata/asthma/cohorts"),
    sqlFolder = here::here("inst/testdata/asthma/sql/sql_server"),
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortId"),
    subsetJsonFolder = here::here("inst/testdata/asthma/cohort_subset_definitions/"),
    #packageName = "HadesExtras",
    verbose = FALSE
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  # cohorts from cohort data
  sourcePersonToPersonId <- helper_getParedSourcePersonAndPersonIds(
    connection = cohortTableHandler$connectionHandler$getConnection(),
    cohortDatabaseSchema = cohortTableHandler$cdmDatabaseSchema,
    numberPersons = 100
  )

  cohort_data <- tibble::tibble(
    cohort_name = rep(c("cohortdata A", "cohortdata B"), 50),
    person_source_value = c(sourcePersonToPersonId$person_source_value[1:90], LETTERS[1:10] ),
    cohort_start_date = rep(as.Date(c("2020-01-01", "2020-01-01")), 50),
    cohort_end_date = rep(as.Date(c(NA, "2020-01-04")), 50)
  )

  cohortDefinitionSet <- cohortDataToCohortDefinitionSet(
    cohortData = cohort_data,
    cohortIdOffset = 10
  )

  cohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

  cohortsSummary <- cohortTableHandler$getCohortsSummary()

  # make one cohort empty
  cohortsSummary <- cohortsSummary |>
    dplyr::mutate(
      cohortEntries = dplyr::if_else(cohortId == 3, NA, cohortEntries),
      cohortSubjects = dplyr::if_else(cohortId == 3, NA, cohortSubjects)
    ) |>
    HadesExtras::correctEmptyCohortsInCohortsSummary()

  # table -------------------------------------------------------------------
  reactableResult <- rectable_cohortsSummary(cohortsSummary)#,  deleteButtonsShinyId = "test")

  reactableResult |> checkmate::expect_class(classes = c("reactable", "htmlwidget"))
  reactableResult$x$tag$attribs$columns |> length() |> expect_equal(7)

  #
  reactableResult <- rectable_cohortsSummary(cohortsSummary,  deleteButtonsShinyId = "test")

  reactableResult |> checkmate::expect_class(classes = c("reactable", "htmlwidget"))
  reactableResult$x$tag$attribs$columns |> length() |> expect_equal(8)

})
