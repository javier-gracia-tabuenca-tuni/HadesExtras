


# webAPI url for the Atlas demo
baseUrl <- "https://api.ohdsi.org/WebAPI"
# A list of cohort IDs for use in this vignette
cohortIds <- c(1783696, 1783697, 1783698)
# Get the SQL/JSON for the cohorts
cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  baseUrl = baseUrl,
  cohortIds = cohortIds
)


CohortGenerator::saveCohortDefinitionSet(
  cohortDefinitionSet,
  settingsFileName = "inst/BasicCohorts/Cohorts.csv",
  jsonFolder = "inst/BasicCohorts/cohorts",
  sqlFolder = "inst/BasicCohorts/sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  subsetJsonFolder = "inst/BasicCohorts/cohort_subset_definitions/",
  verbose = FALSE
)






