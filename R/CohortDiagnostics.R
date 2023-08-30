
#' CohortDiagnostics_runTimeCodeWAS
#'
#' This function calculates the number of subjects with observation case and controls in each time window for a given cohort. It returns a data frame with the counts of cases and controls for each time window and covariate, as well as the results of a Fisher's exact test comparing the counts of cases and controls.
#'
#' @param connectionDetails A list of details needed to create a database connection, or a pre-existing database connection object.
#' @param connection A pre-existing database connection object.
#' @param cdmDatabaseSchema The name of the schema containing the Common Data Model (CDM) tables.
#' @param vocabularyDatabaseSchema The name of the schema containing the vocabulary tables.
#' @param cohortDatabaseSchema The name of the schema containing the cohort table.
#' @param cohortTable The name of the cohort table.
#' @param cohortIdCases The ID of the cohort representing cases.
#' @param cohortIdControls The ID of the cohort representing controls.
#' @param covariateSettings A list of settings for extracting temporal covariates.
#'
#' @return A data frame with the counts of cases and controls for each time window and covariate, as well as the results of a Fisher's exact test comparing the counts of cases and controls.
#'
#' @importFrom DatabaseConnector connect disconnect dateAdd
#' @importFrom FeatureExtraction getDbCovariateData createDefaultTemporalCovariateSettings
#' @importFrom checkmate assertString assertNumeric assertList
#' @importFrom dplyr tbl filter left_join cross_join group_by count collect mutate
#' @importFrom tidyr spread
#' @importFrom tibble as_tibble
#' @importFrom stringr str_c
#'
#' @export
#'
CohortDiagnostics_runTimeCodeWAS <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable = "cohort",
    cohortIdCases,
    cohortIdControls,
    covariateSettings = FeatureExtraction::createDefaultTemporalCovariateSettings()
) {
  #
  # Check parameters
  #
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  cdmDatabaseSchema |> checkmate::assertString()
  vocabularyDatabaseSchema |> checkmate::assertString()
  cohortDatabaseSchema |> checkmate::assertString()
  cohortTable |> checkmate::assertString()
  cohortIdCases |> checkmate::assertNumeric()
  cohortIdControls |> checkmate::assertNumeric()
  covariateSettings |> checkmate::assertList()

  #
  # function
  #
  ParallelLogger::logInfo("Getting FeatureExtraction for cases")
  covariate_case <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortId = cohortIdCases,
    aggregated = T
  )
  ParallelLogger::logInfo("Getting FeatureExtraction for controls")
  covariate_control <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    covariateSettings = covariateSettings,
    cohortId = cohortIdControls,
    aggregated = T
  )

  ParallelLogger::logInfo("calcualting number of subjects with observation case and controls in each time window")

  cohortTbl <- dplyr::tbl(connection, tmp_inDatabaseSchema(cohortDatabaseSchema, cohortTable))
  observationPeriodTbl <- dplyr::tbl(connection, tmp_inDatabaseSchema(cdmDatabaseSchema, "observation_period"))

  timeWindows <- tibble(
    id_window =  as.integer(1:length(covariateSettings$temporalStartDays)),
    start_days = as.integer(covariateSettings$temporalStartDays),
    end_days = as.integer(covariateSettings$temporalEndDays)
  )
  timeWindowsTbl <- tmp_dplyr_copy_to(connection, timeWindows, overwrite = TRUE)

  windowCounts <- cohortTbl |>
    dplyr::filter(cohort_definition_id %in% c(cohortIdCases, cohortIdControls)) |>
    dplyr::left_join(
      # at the moment take the first and last
      observationPeriodTbl |>
        dplyr::select(person_id, observation_period_start_date, observation_period_end_date) |>
        dplyr::group_by(person_id) |>
        dplyr::summarise(
          observation_period_start_date = min(observation_period_start_date, na.rm = TRUE),
          observation_period_end_date = max(observation_period_end_date, na.rm = TRUE)
        ),
      by = c("subject_id" = "person_id")) |>
    dplyr::cross_join(timeWindowsTbl) |>
    dplyr::filter(
      # exclude if window is under the observation_period_start_date or over the observation_period_end_date
      !(dateAdd("day", end_days, cohort_start_date) < observation_period_start_date |
          dateAdd("day", start_days, cohort_start_date) > observation_period_end_date )
    ) |>
    dplyr::group_by(cohort_definition_id, id_window) |>
    dplyr::count() |>
    dplyr::collect()



  windowCounts <-  windowCounts |>
    dplyr::mutate(cohort_definition_id = dplyr::case_when(
      cohort_definition_id == cohortIdCases ~ "n_cases",
      cohort_definition_id == cohortIdControls ~ "n_controls",
      TRUE ~ as.character(NA)
    )) |>
    tidyr::spread(key = cohort_definition_id, n)|>
    dplyr::rename(timeId = id_window)


  ParallelLogger::logInfo("formating output")

  timeCodeWasCounts <-
    dplyr::full_join(
      covariate_case$covariates |> tibble::as_tibble() |>
        select(covariateId, timeId, n_cases_yes=sumValue),
      covariate_control$covariates |> tibble::as_tibble() |>
        select( covariateId, timeId, n_controls_yes=sumValue),
      by = c("covariateId", "timeId")
    )  |>
    dplyr::left_join(
      windowCounts,
      by="timeId"
    ) |>
    dplyr::mutate(
      n_cases_yes = dplyr::if_else(is.na(n_cases_yes), 0, n_cases_yes),
      n_controls_yes = dplyr::if_else(is.na(n_controls_yes), 0, n_controls_yes),
      n_controls = n_controls,
      n_cases = n_cases,
      n_cases_no = n_cases - n_cases_yes,
      n_controls_no = n_controls - n_controls_yes
    ) |>
    dplyr::left_join(
      covariate_case$timeRef |>  tibble::as_tibble() |>
        dplyr::mutate(timeRange = stringr::str_c("from ", startDay," to ", endDay)) |>
        dplyr::arrange(timeId) |>
        dplyr::select(timeId, timeRange),
      by = "timeId"
    ) |>
    dplyr::left_join(
      dplyr::bind_rows(
        covariate_case$covariateRef |> tibble::as_tibble(),
        covariate_control$covariateRef |> tibble::as_tibble()
      ) |>
        dplyr::distinct(covariateId, covariateName ),
      by = "covariateId"
    )

  ParallelLogger::logInfo("Adding Fisher test output")
  timeCodeWasCounts <- .addFisherTestToCodeCounts(timeCodeWasCounts)

  ParallelLogger::logInfo("CohortDiagnostics_runTimeCodeWAS completed")

  return(timeCodeWasCounts)

}





#' @title addFisherTestToCodeCounts
#' @description performs an Fisher test in each row of the case_controls_counts table
#' @param case_controls_counts table obtained from `getCodeCounts`.
#' @return inputed table with appened columsn :
#' - p : for the p-value
#' - OR; for the ods ratio
#' @export
#' @importFrom dplyr setdiff bind_cols select mutate if_else
#' @importFrom purrr pmap_df
.addFisherTestToCodeCounts <- function(timeCodeWasCounts){

  missing_collumns <- dplyr::setdiff(c("n_cases_yes","n_cases_no","n_controls_yes","n_controls_no"), names(timeCodeWasCounts))
  if(length(missing_collumns)!=0){
    stop("case_controls_counts is missing the following columns: ", missing_collumns)
  }


  timeCodeWasCounts <- timeCodeWasCounts |>
    dplyr::bind_cols(
      timeCodeWasCounts |>
        dplyr::select(n_cases_yes,n_cases_no,n_controls_yes,n_controls_no) |>
        purrr::pmap_df( ~.fisher(..1,..2,..3,..4))
    ) |>
    dplyr::mutate(
      up_in = dplyr::if_else(OR>1, "Case", "Ctrl")
    )

  return(timeCodeWasCounts)

}



.fisher <- function(a,b,c,d){
  data <-matrix(c(a,b,c,d),ncol=2)
  fisher_results <- stats::fisher.test(data)
  return(list(
    p = fisher_results$p.value,
    OR = fisher_results$estimate
  ))
}



