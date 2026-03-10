library(tidyverse)
library(fs)
# library(readxl)
# library(rmarkdown)
# library(janitor)
library(jsonlite)
library(ckanr)

# Helper functions ==================================

# Writes a CSV file to the "output/" directory, and returns the data if necessary for future piped functions.
write_out_csv <- function(df, filename, na = "") {
  
  df %>%
    write_csv(
      str_c("output/", filename, ".csv"), 
      na = na
      # ,
      # eol = "\r\n",
    )
  
  df
  
}



run_log <- tribble(
  ~time, ~message
)

# Logging helper function
add_log_entry <- function(log_text) {
  
  new_row = tibble_row(
    time = now(),
    message = log_text
  )
  
  run_log <<- run_log |>
    bind_rows(
      new_row
    )
  
  cat(log_text, "\n")
}


# CKAN initial setup ------------------------------------------------------

run_start_time <- now()
add_log_entry(str_c("Start time was: ", run_start_time))

if(file_exists(".env")) {
  readRenviron(".env")
  
  ckan_url <- Sys.getenv("ckan_url")
  ckan_api_token <- Sys.getenv("ckan_api_token")
  
  ckan_open_government_portal_metrics_package_id <- Sys.getenv("ckan_open_government_portal_metrics_package_id")
  
  matomo_auth_token <- Sys.getenv("matomo_auth_token")
  
  matomo_earliest_date <- Sys.getenv("matomo_earliest_date")
  
} else {
  stop("No .env file found, create it before running this script.")
}

ckanr_setup(
  url = ckan_url, 
  key = ckan_api_token
)


# Retrieve Matomo metrics via API -----------------------------------------

get_matomo_visit_and_pageview_metrics <- function() {
  
  
  get_matomo_multisites_stats <- function(month = "2026-01") {
    
    # api_query <- str_c("https://analytics.gov.yk.ca/?module=API&method=MultiSites.getAllWithGroups&period=day&date=yesterday&pattern=&format=JSON&token_auth=", matomo_auth_token)
    api_query <- str_c("https://analytics.gov.yk.ca/?module=API&method=MultiSites.getAllWithGroups&period=month&date=", month, "&pattern=&format=JSON&token_auth=", matomo_auth_token)
    
    # Don't log the auth token in a CSV output!
    add_log_entry(str_c("Calling ", str_sub(api_query, 0L, 40L), "\n"))
    
    matomo_response <- fromJSON(
      api_query
    )
    
    # matomo_response
    
    multisite_stats <- tibble(site_data = matomo_response$sites) |> 
      unnest_wider(site_data) |> 
      mutate(
        month = month
      ) |> 
      select(
        month,
        label,
        main_url,
        nb_visits,
        nb_pageviews,
        nb_actions
      ) |> 
      rename(
        visits = "nb_visits",
        pageviews = "nb_pageviews",
        actions = "nb_actions",
        
      )
    
    multisite_stats
    
  }
  
  get_month_range <- function() {
    
    date_range <- tibble(
      start_date = as.Date(matomo_earliest_date),
      end_date = as.Date(now())
    )
    
    # Thanks Google
    date_range <- date_range |> 
      mutate(
        month_date = map2(start_date, end_date, ~seq(.x, .y, by = "month"))
      ) |> 
      unnest(month_date) |> 
      select(month_date)
    
    date_range <- date_range |> 
      mutate(
        month_date = str_sub(as.character(month_date), 0L, 7L)
      )
    
    date_range |> 
      pull(month_date)
    
  }
  
  
  get_matomo_multisites_stats_all_months <- function() {
    
    months <- get_month_range()
    # For testing purposes, set to NULL to run every available month:
    max_requests <- NULL
    
    output <- tibble()
    
    for (i in seq_along(months)) { 
      
      add_log_entry(str_c("Requesting ", i, ": ", as.character(months[[i]])))
      
      current_month_stats <- get_matomo_multisites_stats(months[[i]])
      
      output <- output |> 
        bind_rows(current_month_stats)
      
      if(is.null(max_requests) == FALSE) {
        if(i >= max_requests) {
          break;
        }
      }
      
      Sys.sleep(1)
      
    }
    
    output
    
  }
  
  postprocess_matomo_multisites_stats <- function(multisites_stats) {
    
    # Filter out site-month entries with 0 visitors that month
    multisites_stats <- multisites_stats |> 
      filter(visits > 0) |> 
      arrange(
        label,
        month
        
      )
    
  }
  
  output <- get_matomo_multisites_stats_all_months()
  
  output <- postprocess_matomo_multisites_stats(output)
  
  # Returns all sites (that the auth token can reach)
  output
  
  
  
}



# Retrieve CKAN package and resource metrics ------------------------------

get_ckan_package_and_resource_metrics <- function() {
  
  # How many datasets to retrive per API call
  offset_increment = 100
  # When to stop (as a safety buffer or for testing, if you're missing results you may need to bump this up!)
  max_runs = 100
  
  # Retrieve a set of CKAN packages (datasets or publications)
  get_package_resource_totals <- function(offset = 0, offset_increment = 10) {
    
    # Fields to keep
    # id, title, num_resources, type, organization
    
    results <- package_list_current(
      as = 'table',
      offset = offset,
      limit = offset_increment
    )
    
    if(length(results) == 0) {
      # We're past the end of the results
      return(results)
    }
    
    results <- results |> 
      select(id, name, title, num_resources, type, organization, metadata_created, metadata_modified) |> 
      unnest(
        organization,
        names_sep = "_"
      ) |> 
      select(id, name, title, num_resources, type, organization_name, metadata_created, metadata_modified)
    
    results  
    
  }
  
  current_offset <- 0
  run_count <- 0
  
  # Loop through API requests, increasing the offset to retrieve the entire collection in the CKAN instance
  loop_get_package_resource_totals <- function() {
    
    while(run_count <= max_runs) {
      
      add_log_entry(str_c("Current CKAN run ", run_count, " starting at ", current_offset))
      
      if(current_offset == 0) {
        # First run
        output <- get_package_resource_totals(current_offset, offset_increment)
      }
      else {
        new_output <- get_package_resource_totals(current_offset, offset_increment)
        
        # If it's an empty list, that means we're past the end of the existing resources
        if(length(new_output) == 0) {
          
          add_log_entry(str_c("Ending CKAN run at ", run_count))
          
          break
        }
        
        output <- output |> 
          bind_rows(new_output)
      }
      
      current_offset <- current_offset + offset_increment
      run_count <- run_count + 1
      
      # Be gentle to the CKAN API between requests!
      Sys.sleep(0.3)
      
    }
    
    output
    
  }
  
  
  # Get all packages (across all dataset and publication types) and combine them into one table:
  output <- loop_get_package_resource_totals()
  
  output
  
}

summarize_ckan_package_and_resource_metrics <- function(ckan_package_output) {
  
  output <- ckan_package_output
  
  # Add year created and year modified
  output <- output |> 
    mutate(
      year_created = str_sub(metadata_created, 0L, 4L),
      year_modified = str_sub(metadata_modified, 0L, 4L),
    )
  
  # Skip "dataset" packages since these aren't categorized properly
  output <- output |> 
    filter(type != "dataset")
  
  packages_by_type <- output |> 
    group_by(
      type
    ) |> 
    summarise(
      packages = n(),
      resources = sum(num_resources)
    )
  
  packages_by_type_by_org <- output |> 
    group_by(
      type,
      organization_name
    ) |> 
    summarise(
      packages = n(),
      resources = sum(num_resources)
    )
  
  packages_by_type_by_year_by_org <- output |> 
    group_by(
      type,
      organization_name, 
      year_created
    ) |> 
    summarise(
      packages = n(),
      resources = sum(num_resources)
    )
  
  packages_by_type_by_year <- output |> 
    group_by(
      type,
      year_created
    ) |> 
    summarise(
      packages = n(),
      resources = sum(num_resources)
    )
  
  # Open data and open information specifically
  open_data_datasets_by_year <- packages_by_type_by_year |> 
    filter(type == "data") |> 
    ungroup() |> 
    select(! type)
  
  open_information_publications_by_year <- packages_by_type_by_year |> 
    filter(type == "information") |> 
    ungroup() |> 
    select(! type)
  
  # TODO: write this as a mappable purrr function
  
  packages_by_type |> 
    write_out_csv("packages_by_type")
  
  packages_by_type_by_org |> 
    write_out_csv("packages_by_type_by_org")
  
  packages_by_type_by_year_by_org |> 
    write_out_csv("packages_by_type_by_year_by_org")
  
  packages_by_type_by_year |> 
    write_out_csv("packages_by_type_by_year")
  
  open_data_datasets_by_year |> 
    write_out_csv("open_data_datasets_by_year")
  
  open_information_publications_by_year |> 
    write_out_csv("open_information_publications_by_year")
  
}




# Upload CSV output to open.yukon.ca via CKAN API -------------------------

upload_metrics_to_ckan <- function() {
  
  metrics_package <- package_show(
    id = "open-government-portal-metrics"
  )
  
  # TODO: match resource filenames (?) with the CSV files in output/
  # and then upload new versions to the equivalent resource.
  
  
  
}
