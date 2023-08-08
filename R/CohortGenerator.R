
#' Get cohort counts and basic cohort demographics for a specific cohort.
#'
#' This function retrieves demographic information for a given cohort .
#'
#' @param connectionDetails Details required to establish a database connection (optional).
#' @param connection An existing database connection (optional).
#' @param cdmDatabaseSchema The schema name for the Common Data Model (CDM) database.
#' @param vocabularyDatabaseSchema The schema name for the vocabulary database (default is \code{cdmDatabaseSchema}).
#' @param cohortDatabaseSchema The schema name for the cohort database.
#' @param cohortTable The name of the cohort table in the cohort database (default is "cohort").
#' @param cohortIds A numeric vector of cohort IDs for which to retrieve demographics (default is an empty vector).
#' @param toGet A character vector indicating which demographic information to retrieve. Possible values include "histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", and "sexCounts".
#' @param cohortDefinitionSet A set of cohort definitions (optional).
#' @param databaseId The ID of the database (optional).
#'
#' @return
#' A data frame with cohort counts and selected demographics
#'
#' @importFrom DatabaseConnector connect disconnect getTableNames
#' @importFrom dplyr tbl count collect mutate left_join distinct select nest_by
#' @importFrom CohortGenerator getCohortCounts
#'
#' @export
CohortGenerator_getCohortDemograpics <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortIds = c(),
    toGet = c("histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "sexCounts"),
    cohortDefinitionSet = NULL,
    databaseId = NULL
    ) {

  start <- Sys.time()

  #
  # validate parameters
  #
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  tablesInServer <- tolower(DatabaseConnector::getTableNames(conn = connection, databaseSchema = cohortDatabaseSchema))
  if (!(tolower(cohortTable) %in% tablesInServer)) {
    warning("Cohort table was not found. Was it created?")
    return(NULL)
  }

  toGet |> checkmate::assertSubset(c( "histogramCohortStartYear", "histogramCohortEndYear", "histogramBirthYear", "sexCounts"))

  #
  # function
  #
  cohortTable <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTable))
  personTable  <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "person"))
  conceptTable  <- dplyr::tbl(connection, tmp_inDatabaseSchema(vocabularyDatabaseSchema, "concept"))

  cohortCounts <- CohortGenerator::getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = cohortIds,
      cohortDefinitionSet = cohortDefinitionSet,
      databaseId = databaseId
    )

  histogramCohortStartYear <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("histogramCohortStartYear" %in% toGet) {
    histogramCohortStartYear <- cohortTable |>
      dplyr::mutate( year = year(cohort_start_date) ) |>
      dplyr::count(cohort_definition_id, year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramCohortStartYear")
  }

  histogramCohortEndYear <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("histogramCohortEndYear" %in% toGet) {
    histogramCohortEndYear <- cohortTable |>
      dplyr::mutate( year = year(cohort_end_date) ) |>
      dplyr::count(cohort_definition_id, year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramCohortEndYear")
  }

  histogramBirthYear <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("histogramBirthYear" %in% toGet) {
    histogramBirthYear <- cohortTable |>
      dplyr::distinct(subject_id) |>
      dplyr::left_join(
        personTable  |> dplyr::select(person_id, year_of_birth),
        by = c("subject_id" = "person_id")
      )|>
      dplyr::mutate( year = year_of_birth ) |>
      dplyr::count(cohort_definition_id, year)  |>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "histogramBirthYear")
  }

  sexCounts <-  tibble::tibble(cohort_definition_id=0, .rows = 0)
  if ("sexCounts" %in% toGet) {
    sexCounts <- cohortTable  |>
      dplyr::left_join(
        personTable  |> dplyr::select(person_id, gender_concept_id),
        by = c("subject_id" = "person_id")
      ) |>
      dplyr::left_join(
        conceptTable |> dplyr::select(concept_id, concept_name),
        by = c("gender_concept_id" = "concept_id")
      ) |>
      dplyr::count(cohort_definition_id, sex=concept_name)|>
      dplyr::collect() |>
      dplyr::nest_by(cohort_definition_id, .key = "count_sex")
  }

  cohortsSummary <- cohortCounts |>
    dplyr::left_join(histogramCohortStartYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(histogramCohortEndYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(histogramBirthYear, by = c("cohortId" = "cohort_definition_id")) |>
    dplyr::left_join(sexCounts, by = c("cohortId" = "cohort_definition_id"))


  ParallelLogger::logInfo(paste("Counting cohorts took", signif(delta, 3), attr(delta, "units")))

  return(cohortDemograpics)

}


#' Delete cohort from cohort table
#'
#' This function deletes specified cohorts from the cohort table and cohort names table in the database.
#'
#' @param connectionDetails A list containing the necessary details to establish a database connection.
#' @param schema The name of the schema where the cohort table and cohort names table are located.
#' @param cohort_table_name The name of the cohort table.
#' @param cohort_names A character vector of cohort names to be deleted.
#'
#' @importFrom DatabaseConnector connect disconnect executeSql
#' @importFrom SqlRender readSql render translate
#'
#' @return TRUE if the deletion is successful.
#'
#' @export
#'
CohortGenerator_deleteCohortFromCohortTable  <- function(
    connectionDetails = NULL,
    connection = NULL,
    cohortDatabaseSchema,
    cohortTableNames,
    cohortIds){
  #
  # Validate parameters
  #
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  checkmate::assertCharacter(cohortDatabaseSchema, len = 1)
  checkmate::assertList(cohortTableNames)
  checkmate::assertNumeric(cohortIds)

  #browser()
  # Function
  sql <- SqlRender::readSql(system.file("sql/sql_server/DeleteCohortFromCohortTables.sql", package = "HadesExtras", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTableNames$cohortTable,
    cohort_ids = paste0("(", paste0(cohortIds, collapse = " ,"), ")"),
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  return(TRUE)
}


#
# Subsets
#

CohortGenerator_MatchingSubsetQb <- R6::R6Class(
  classname = "MatchingSubsetQb",
  inherit = CohortGenerator:::QueryBuilder,
  private = list(
    innerQuery = function(targetTable) {
      sql <- SqlRender::readSql(system.file("sql", "sql_server", "subsets", "MatchingSubsetOperator.sql", package = "HadesExtras"))
      sql <- SqlRender::render(sql,
                               target_table = targetTable,
                               output_table = self$getTableObjectId(),
                               match_to_cohort_id = private$operator$matchToCohortId,
                               match_ratio = private$operator$matchRatio,
                               match_sex = ifelse(private$operator$matchSex == TRUE, yes = "1", no = "0"),
                               match_birth_year = ifelse(private$operator$matchBirthYear == TRUE, yes = "1", no = "0"),
                               match_start_date_with_in_duration = ifelse(private$operator$matchCohortStartDateWithInDuration == TRUE, yes = "1", no = "0"),
                               new_cohort_start_date_as_match = ifelse(private$operator$newCohortStartDate == "asMatch", yes = "1", no = "0"),
                               new_cohort_end_date_as_match = ifelse(private$operator$newCohortStartDate == "asMatch", yes = "1", no = "0"),
                               warnOnMissingParameters = TRUE
      )
      return(sql)
    }
  )
)

# MatchingSubsetOperator ------------------------------
#' @title Matching Subset Operator
#' @export
#' @description
#' A subset of type cohort - subset a population to only those contained within defined cohort
CohortGenerator_MatchingSubsetOperator <- R6::R6Class(
  classname = "MatchingSubsetOperator",
  inherit = CohortGenerator::SubsetOperator,
  private = list(
    suffixStr = "Match",
    queryBuilder = CohortGenerator_MatchingSubsetQb,
    .matchToCohortId = integer(0),
    .matchRatio = 10,
    .matchSex = TRUE,
    .matchBirthYear = TRUE,
    .matchCohortStartDateWithInDuration = FALSE,
    .newCohortStartDate = "keep",
    .newCohortEndDate = "keep"
  ),
  public = list(
    #' to List
    #' @description List representation of object
    toList = function() {
      objRepr <- super$toList()
      objRepr$matchToCohortId <- private$.matchToCohortId
      objRepr$matchRatio <- private$.matchRatio
      objRepr$matchSex <- private$.matchSex
      objRepr$matchBirthYear <- private$.matchBirthYear
      objRepr$matchCohortStartDateWithInDuration <- private$.matchCohortStartDateWithInDuration
      objRepr$newCohortStartDate <- private$.newCohortStartDate
      objRepr$newCohortEndDate  <- private$.newCohortEndDate

      objRepr
    },
    #' Get auto generated name
    #' @description name generated from subset operation properties
    #'
    #' @return character
    getAutoGeneratedName = function() {
      nameString <- paste0("Match to cohort ", private$.matchToCohortId, " by" )

      matchBy <- as.character()
      if (private$.matchSex) { matchBy <- append(matchBy,"sex") }
      if (private$.matchBirthYear) { matchBy <- append(matchBy,"birth year") }
      if (private$.matchCohortStartDateWithInDuration) { matchBy <- append(matchBy,"cohort start date within cohort duration") }

      nameString <- paste(nameString, paste(matchBy, collapse = " and "))

      nameString <- paste0(nameString, " with ratio 1:", private$.matchRatio)
      if (private$.newCohortStartDate == "asMatch") {
        nameString <- paste0(nameString, "; cohort start date as in matched subject")
      }
      if (private$.newCohortEndDate == "asMatch" ) {
        nameString <- paste0(nameString, "; cohort end date as in matched subject")
      }

      return(paste0(nameString))
    }
  ),
  active = list(
    #' @field matchToCohortId Integer id of cohorts to match to
    matchToCohortId = function(matchToCohortId) {
      if (missing(matchToCohortId)) {
        return(private$.matchToCohortId)
      }

      matchToCohortId <- as.integer(matchToCohortId)
      checkmate::assertIntegerish(matchToCohortId, len = 1)
      checkmate::assertFALSE(any(is.na(matchToCohortId)))
      private$.matchToCohortId <- matchToCohortId
      self
    },
    #' @field matchRatio matching ratio
    matchRatio = function(matchRatio) {
      if (missing(matchRatio)) {
        return(private$.matchRatio)
      }

      checkmate::assertIntegerish(matchRatio, len = 1)
      private$.matchRatio <- matchRatio
      self
    },
    #' @field matchSex match to target sex
    matchSex = function(matchSex) {
      if (missing(matchSex)) {
        return(private$.matchSex)
      }

      checkmate::assertLogical(matchSex, len = 1)
      private$.matchSex <- matchSex
      self
    },
    #' @field matchBirthYear match to target birth year
    matchBirthYear = function(matchBirthYear) {
      if (missing(matchBirthYear)) {
        return(private$.matchBirthYear)
      }

      checkmate::assertLogical(matchBirthYear, len = 1)
      private$.matchBirthYear <- matchBirthYear
      self
    },
    #' @field matchCohortStartDateWithInDuration match have start date as in cohort duration
    matchCohortStartDateWithInDuration = function(matchCohortStartDateWithInDuration) {
      if (missing(matchCohortStartDateWithInDuration)) {
        return(private$.matchCohortStartDateWithInDuration)
      }

      checkmate::assertLogical(matchCohortStartDateWithInDuration, len = 1)
      private$.matchCohortStartDateWithInDuration <- matchCohortStartDateWithInDuration
      self
    },
    #' @field newCohortStartDate change cohort start date to the matched person
    newCohortStartDate = function(newCohortStartDate) {
      if (missing(newCohortStartDate)) {
        return(private$.newCohortStartDate)
      }

      checkmate::assertChoice(x = newCohortStartDate, choices = c("keep", "asMatch"))
      private$.newCohortStartDate <- newCohortStartDate
      self
    },
    #' @field newCohortEndDate  change cohort end date to the matched person
    newCohortEndDate  = function(newCohortEndDate ) {
      if (missing(newCohortEndDate )) {
        return(private$.newCohortEndDate )
      }

      checkmate::assertChoice(x = newCohortEndDate, choices = c("keep", "asMatch"))
      private$.newCohortEndDate  <- newCohortEndDate
      self
    }
  )
)

# createMatchingSubset ------------------------------
#' A definition of subset functions to be applied to a set of cohorts
#' @export
#'
#' @param name  optional name of operator
#' @param matchToCohortId integer - cohort ids to match to
#' @param matchSex match to target sex
#' @param matchBirthYear match to target birth year
#' @param matchRatio matching ratio
#' @param newCohortStartDate change cohort start date to the matched person
#' @param newCohortEndDate  change cohort end date to the matched person
#'
#' @returns a MatchingSubsetOperator instance
createMatchingSubset <- function(
    name = NULL,
    matchToCohortId,
    matchRatio = 10,
    matchSex = TRUE,
    matchBirthYear = TRUE,
    matchCohortStartDateWithInDuration = FALSE,
    newCohortStartDate = "keep",
    newCohortEndDate = "keep") {

  if(!matchSex & !matchBirthYear){
    stop("Either matchSex or matchBirthYear has to be chosen, both cannot be FALSE")
  }

  subset <- CohortGenerator_MatchingSubsetOperator$new()
  subset$name <- name
  subset$matchToCohortId <- matchToCohortId
  subset$matchRatio <- matchRatio
  subset$matchSex <- matchSex
  subset$matchCohortStartDateWithInDuration <- matchCohortStartDateWithInDuration
  subset$matchBirthYear <- matchBirthYear
  subset$newCohortStartDate <- newCohortStartDate
  subset$newCohortEndDate <- newCohortEndDate

  subset
}

