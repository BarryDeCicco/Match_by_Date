
library(tidyverse)
library(lubridate)
library(magrittr)
library(fuzzyjoin)
library(powerjoin)
library(sqldf)

# From:  https://stackoverflow.com/questions/71860379/r-dplyr-join-tables-based-on-overlapping-date-time-intervals
# Jon Spring's answer:

# Label, start - end time
have_label_start_end <- data.frame(label = c('X547', 'X285', 'X290')
                                   , time = c(lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 9L, min = 58L, sec = 51, tz = "UTC"),
                                              lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 10L, min = 4L, sec = 54, tz = "UTC"),
                                              lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 11L, min = 8L, sec = 34, tz = "UTC"))) %>%
  dplyr::mutate(start_time = time, stop_time = lead(time)) %>% dplyr::select(-time)



  # Item
  have_item_time <- data.frame(item = c('a123', 'b682', 'c3324', 'd4343', 'e5343')
                               , timestamp = c(lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 9L, min = 59L, sec = 34, tz = "UTC"),
                                               lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 10L, min = 3L, sec = 13, tz = "UTC"),
                                               lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 10L, min = 5L, sec = 17, tz = "UTC"),
                                               lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 11L, min = 8L, sec = 35, tz = "UTC"),
                                               lubridate::make_datetime(year = 2022L, month = 4L, day = 11L, hour = 11L, min = 10L, sec = 09, tz = "UTC")))






have_item_time %>%
  fuzzy_left_join(have_label_start_end %>%
                    replace_na(list(stop_time = max(have_item_time$timestamp))),
                  by = c("timestamp" = "start_time",
                         "timestamp" = "stop_time"),
                  match_fun = list(`>=`, `<=`))

# From:  https://stackoverflow.com/questions/71860379/r-dplyr-join-tables-based-on-overlapping-date-time-intervals
# moodymudskipper's answer:

power_left_join(
  have_item_time, have_label_start_end,
  by = ~.x$timestamp > .y$start_time &
    (.x$timestamp < .y$stop_time | is.na(.y$stop_time)),
  keep = "left")


# From: https://community.rstudio.com/t/tidy-way-to-range-join-tables-on-an-interval-of-dates/7881

suppressPackageStartupMessages(library(tidyverse))

df1 <- tibble::tribble(
  ~id, ~category,       ~date,
  1L,       "a",  "7/1/2000",
  2L,       "b", "11/1/2000",
  3L,       "c",  "7/1/2002"
) %>% mutate(date = as.Date(date, format = "%m/%d/%Y"))

df2 <- tibble::tribble(
  ~category, ~other_info,     ~start,         ~end,
  "a",         "x", "1/1/2000", "12/31/2000",
  "b",         "y", "1/1/2001", "12/31/2001",
  "c",         "z", "1/1/2002", "12/31/2002"
) %>% mutate_at(vars(start, end), as.Date, format = "%m/%d/%Y")

df1
#> # A tibble: 3 x 3
#>      id category date
#>   <int> <chr>    <date>
#> 1     1 a        2000-07-01
#> 2     2 b        2000-11-01
#> 3     3 c        2002-07-01
df2
#> # A tibble: 3 x 4
#>   category other_info start      end
#>   <chr>    <chr>      <date>     <date>
#> 1 a        x          2000-01-01 2000-12-31
#> 2 b        y          2001-01-01 2001-12-31
#> 3 c        z          2002-01-01 2002-12-31
#>
#>
#>

sqldf::sqldf("SELECT a.id, a.category, b.other_info, a.date, b.start, b.end
      FROM df1 a
      LEFT JOIN df2 b on a.category = b.category AND
      a.date >= b.start AND a.date <= b.end") %>%
  as_tibble()

