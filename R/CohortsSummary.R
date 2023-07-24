#' Create an empty cohorts summary
#'
#' @return A tibble with cohort summary structure
#' @importFrom tibble tibble
#' @export
#'
createEmptyCohortsSummary <- function() {

  emptyCohortsSummary <- tibble::tibble(
    cohortName = as.character(NA),
    cohortId = as.double(NA),
    sourceCohortId = as.double(NA),
    cohortDescription = as.character(NA),
    cohortEntries = as.integer(NA),
    cohortSubjects = as.integer(NA),
    histogram_cohort_start_year = list(
      tibble::tibble(
        year = as.integer(NA),
        n = as.integer(NA),
        .rows = 0)
      ),
    histogram_cohort_end_year = list(
      tibble::tibble(
        year = as.integer(NA),
        n = as.integer(NA),
        .rows = 0)
      ),
    count_sex = list(
      tibble::tibble(
        sex = as.character(NA),
        n = as.integer(NA),
        .rows = 0)),
    .rows = 0 )

  return(emptyCohortsSummary)
}

#' Check if a tibble is of CohortsSummary format.
#'
#' This function performs various validations on the CohortsSummary tibble to ensure its integrity and correctness. The validations include:
#'
#' - Checking if CohortsSummary is a tibble.
#' - Checking if the CohortsSummary tibble contains the required column names
#' - Validating the types
#'
#' @param tibble The tibble to be checked
#'
#' @return TRUE if the tibble is of CohortsSummary format, an array of strings with the failed checks
#' @export
#'
checkCohortsSummary  <- function(tibble) {
  collection <- .assertCollectionCohortsSummary(tibble)
  if (collection$isEmpty()) {
    return(TRUE)
  } else {
    return(collection$getMessages())
  }
}


#' @export
#' @importFrom checkmate reportAssertions
#' @rdname checkCohortsSummary
assertCohortsSummary  <- function(tibble) {
  collection <- .assertCollectionCohortsSummary(tibble)
  if (!collection$isEmpty()) {
    checkmate::reportAssertions(collection)
  }
}

#' Internal function to validate cohort data
#'
#' @param CohortsSummary The cohort data tibble to be validated
#'
#' @importFrom checkmate assertTibble
#' @importFrom validate check_that
#' @importFrom dplyr filter mutate if_else pull
#' @importFrom purrr map
#' @importFrom stringr str_replace_all
.assertCollectionCohortsSummary <- function(CohortsSummary) {

  collection = checkmate::makeAssertCollection()

  CohortsSummary |> checkmate::assertTibble()
  # check column names
  missingCollumnNames <- setdiff(
    createEmptyCohortsSummary() |>  names(),
    CohortsSummary |> names()
  )
  if(length(missingCollumnNames)){
    paste("Table is missing the following columns: ", paste0(missingCollumnNames, collapse = ", ")) |>
      collection$push()
  }
  # validate
  failsNames <- CohortsSummary |> validate::check_that(
    # type
    cohortName.is.not.of.type.character = is.character(cohortName),
    cohortId.is.not.of.type.double = is.double(cohortId),
    sourceCohortId.is.not.of.type.double = is.double(sourceCohortId),
    cohortDescription.is.not.of.type.character = is.character(cohortDescription),
    cohortEntries.is.not.of.type.integer = is.integer(cohortEntries),
    cohortSubjects.is.not.of.type.integer = is.integer(cohortSubjects),
    histogram_cohort_start_year.is.not.of.type.list = is.list(histogram_cohort_start_year),
    histogram_cohort_end_year.is.not.of.type.list  = is.list(histogram_cohort_end_year),
    count_sex.is.not.of.type.list = is.list(count_sex)
    # missing
    # TODO
  ) |>
    validate::summary() |>
    dplyr::filter(fails!=0) |>
    dplyr::mutate(msg = dplyr::if_else(items > 1, as.character(fails), "")) |>
    dplyr::mutate(msg = paste(msg, stringr::str_replace_all(name ,"\\.", " "))) |>
    dplyr::pull(msg)

  # add to collection
  failsNames |> purrr::map(.f=~collection$push(.x))

  return(collection)

}

