source("lib/helpers.R")

# Verifies that the .env values are sufficient to reach CKAN and Matomo.
# Only read-only API calls are made; nothing is created, updated, or deleted.

test_ckan_connection <- function() {

  add_log_entry("Testing CKAN connection...")

  if (ckan_url == "" || ckan_api_token == "") {
    add_log_entry("FAIL: ckan_url or ckan_api_token is missing from .env")
    return(FALSE)
  }

  result <- tryCatch({

    # Read-only call: confirms the URL, API token, and metrics package ID all work together.
    metrics_package <- package_show(id = "open-government-portal-metrics")

    add_log_entry(str_c(
      "PASS: connected to CKAN and found package '", metrics_package$name,
      "' with ", length(metrics_package$resources), " resource(s)"
    ))

    TRUE

  }, error = function(e) {
    add_log_entry(str_c("FAIL: could not reach CKAN or read the metrics package - ", conditionMessage(e)))
    FALSE
  })

  result

}

test_matomo_connection <- function() {

  add_log_entry("Testing Matomo connection...")

  if (matomo_auth_token == "" || matomo_earliest_date == "") {
    add_log_entry("FAIL: matomo_auth_token or matomo_earliest_date is missing from .env")
    return(FALSE)
  }

  parsed_earliest_date <- tryCatch(as.Date(matomo_earliest_date), error = function(e) NA)

  if (is.na(parsed_earliest_date)) {
    add_log_entry(str_c("FAIL: matomo_earliest_date '", matomo_earliest_date, "' is not a valid date"))
    return(FALSE)
  }

  result <- tryCatch({

    # Read-only call: smallest possible request (today only) just to confirm the token works.
    api_query <- str_c(
      "https://analytics.gov.yk.ca/?module=API&method=MultiSites.getAllWithGroups",
      "&period=day&date=today&pattern=&format=JSON&token_auth=", matomo_auth_token
    )

    add_log_entry(str_c("Calling ", str_sub(api_query, 0L, 40L), "\n"))

    matomo_response <- fromJSON(api_query)

    if (!is.null(matomo_response$result) && matomo_response$result == "error") {
      stop(matomo_response$message)
    }

    if (!"sites" %in% names(matomo_response) || length(matomo_response$sites) == 0) {
      stop("no sites returned - token may not have access to any sites")
    }

    add_log_entry(str_c(
      "PASS: connected to Matomo and found ", length(matomo_response$sites), " accessible site(s)"
    ))

    TRUE

  }, error = function(e) {
    add_log_entry(str_c("FAIL: could not reach Matomo or the auth token was rejected - ", conditionMessage(e)))
    FALSE
  })

  result

}

ckan_ok <- test_ckan_connection()
matomo_ok <- test_matomo_connection()

if (ckan_ok && matomo_ok) {
  add_log_entry("All connection tests passed.")
} else {
  add_log_entry("One or more connection tests failed - see above.")
  quit(save = "no", status = 1)
}
