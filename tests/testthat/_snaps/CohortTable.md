# getCohortTableSummary works

    Code
      cohortTableSummary
    Output
      # A tibble: 2 x 6
        cohort_definition_id cohort_name atlas_cohort_definit~1 histogram_cohort_sta~2
                       <dbl> <chr>                        <dbl>     <list<tibble[,2]>>
      1                    1 A                               NA                [1 x 2]
      2                    2 B                               NA                [1 x 2]
      # i abbreviated names: 1: atlas_cohort_definition_id,
      #   2: histogram_cohort_start_year
      # i 2 more variables: histogram_cohort_end_year <list<tibble[,2]>>,
      #   count_sex <list<tibble[,2]>>

# appendCohortDataToCohortTable works

    Code
      cohortTable
    Output
      # A tibble: 14 x 4
         cohort_definition_id subject_id cohort_start_date cohort_end_date
                        <dbl>      <dbl> <date>            <date>         
       1                    1          1 2000-01-02        2020-01-02     
       2                    1          2 2000-01-03        2020-01-03     
       3                    1          3 2000-01-04        2020-01-04     
       4                    1          4 2000-01-05        2020-01-05     
       5                    1          5 2000-01-06        2020-01-06     
       6                    2          6 2000-01-07        2020-01-07     
       7                    2          7 2000-01-08        2020-01-08     
       8                    2          8 2000-01-09        2020-01-09     
       9                    2          9 2000-01-10        2020-01-10     
      10                    2         10 2000-01-11        2020-01-11     
      11                    3          1 2000-01-01        2010-01-03     
      12                    3          2 2010-01-01        2020-01-04     
      13                    4          3 2000-01-01        2010-01-03     
      14                    4          5 2010-01-01        2020-01-04     

---

    Code
      cohortInfoTable
    Output
      # A tibble: 4 x 3
        cohort_definition_id cohort_name atlas_cohort_definition_id
                       <dbl> <chr>                            <dbl>
      1                    1 A                                   NA
      2                    2 B                                   NA
      3                    3 C                                   NA
      4                    4 D                                   NA

