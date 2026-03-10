source("lib/helpers.R")

matomo_output <- get_matomo_visit_and_pageview_metrics()

# Skip the current month since it's incomplete
current_month <- str_sub(as.character(now()), 0L, 7L)

open_government_portal_matomo_output <- matomo_output |> 
  filter(label == "open.yukon.ca") |> 
  filter(month != current_month)

open_government_portal_matomo_output |> 
  write_out_csv("matomo_visits_and_pageviews_open_government_portal")

ckan_package_output <- get_ckan_package_and_resource_metrics()

summarize_ckan_package_and_resource_metrics(ckan_package_output)

