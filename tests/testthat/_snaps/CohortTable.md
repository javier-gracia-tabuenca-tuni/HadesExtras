# getCohortTableSummary works

    Code
      cohort_names
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
      cohort_table
    Output
      # A tibble: 8 x 4
        cohort_definition_id subject_id cohort_start_date cohort_end_date
                       <dbl>      <dbl> <date>            <date>         
      1                    1          1 2000-01-02        2020-01-02     
      2                    1          2 2000-01-03        2020-01-03     
      3                    2          3 2000-01-04        2020-01-04     
      4                    2          4 2000-01-05        2020-01-05     
      5                    3         35 2000-01-01        2010-01-03     
      6                    3         36 2010-01-01        2020-01-04     
      7                    4         41 2000-01-01        2010-01-03     
      8                    4         42 2010-01-01        2020-01-04     

---

    Code
      cohort_names_table
    Output
      # A tibble: 4 x 3
        cohort_definition_id cohort_name atlas_cohort_definition_id
                       <dbl> <chr>                            <dbl>
      1                    1 A                                   NA
      2                    2 B                                   NA
      3                    3 C                                   NA
      4                    4 D                                   NA

# appendCohortDataToCohortTable when cohort_start_date fills in with onservation table

    Code
      cohort_table
    Output
      # A tibble: 8 x 4
        cohort_definition_id subject_id cohort_start_date cohort_end_date
                       <dbl>      <dbl> <date>            <date>         
      1                    1          1 2000-01-02        2020-01-02     
      2                    1          2 2000-01-03        2020-01-03     
      3                    2          3 2000-01-04        2020-01-04     
      4                    2          4 2000-01-05        2020-01-05     
      5                    3         35 1960-03-22        2010-01-03     
      6                    3         36 1958-10-21        2020-01-04     
      7                    4         41 2000-01-01        2010-01-03     
      8                    4         42 2010-01-01        2020-01-04     

---

    Code
      cohort_names_table
    Output
      # A tibble: 4 x 3
        cohort_definition_id cohort_name atlas_cohort_definition_id
                       <dbl> <chr>                            <dbl>
      1                    1 A                                   NA
      2                    2 B                                   NA
      3                    3 C                                   NA
      4                    4 D                                   NA

# appendCohortDataToCohortTable when cohort_end_date fills in with onservation table

    Code
      cohort_table
    Output
      # A tibble: 8 x 4
        cohort_definition_id subject_id cohort_start_date cohort_end_date
                       <dbl>      <dbl> <date>            <date>         
      1                    1          1 2000-01-02        2020-01-02     
      2                    1          2 2000-01-03        2020-01-03     
      3                    2          3 2000-01-04        2020-01-04     
      4                    2          4 2000-01-05        2020-01-05     
      5                    3         35 2000-01-01        2018-12-25     
      6                    3         36 2010-01-01        2019-01-15     
      7                    4         41 2000-01-01        2000-01-01     
      8                    4         42 2010-01-01        2010-01-01     

---

    Code
      cohort_names_table
    Output
      # A tibble: 4 x 3
        cohort_definition_id cohort_name atlas_cohort_definition_id
                       <dbl> <chr>                            <dbl>
      1                    1 A                                   NA
      2                    2 B                                   NA
      3                    3 C                                   NA
      4                    4 D                                   NA

# appendCohortDataToCohortTable when a cohort is missing all the source ids warns 

    Code
      cohort_table
    Output
      # A tibble: 6 x 4
        cohort_definition_id subject_id cohort_start_date cohort_end_date
                       <dbl>      <dbl> <date>            <date>         
      1                    1          1 2000-01-02        2020-01-02     
      2                    1          2 2000-01-03        2020-01-03     
      3                    2          3 2000-01-04        2020-01-04     
      4                    2          4 2000-01-05        2020-01-05     
      5                    4         41 2000-01-01        2010-01-03     
      6                    4         42 2010-01-01        2020-01-04     

---

    Code
      cohort_names_table
    Output
      # A tibble: 3 x 3
        cohort_definition_id cohort_name atlas_cohort_definition_id
                       <dbl> <chr>                            <dbl>
      1                    1 A                                   NA
      2                    2 B                                   NA
      3                    4 D                                   NA

# appendCohortDataToCohortTable when a cohort is missing some the source ids warns 

    Code
      cohort_table
    Output
      # A tibble: 7 x 4
        cohort_definition_id subject_id cohort_start_date cohort_end_date
                       <dbl>      <dbl> <date>            <date>         
      1                    1          1 2000-01-02        2020-01-02     
      2                    1          2 2000-01-03        2020-01-03     
      3                    2          3 2000-01-04        2020-01-04     
      4                    2          4 2000-01-05        2020-01-05     
      5                    3         36 2010-01-01        2020-01-04     
      6                    4         41 2000-01-01        2010-01-03     
      7                    4         42 2010-01-01        2020-01-04     

---

    Code
      cohort_names_table
    Output
      # A tibble: 4 x 3
        cohort_definition_id cohort_name atlas_cohort_definition_id
                       <dbl> <chr>                            <dbl>
      1                    1 A                                   NA
      2                    2 B                                   NA
      3                    3 C                                   NA
      4                    4 D                                   NA

