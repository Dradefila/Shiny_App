library(dplyr)
library(readr)
library(purrr)
library(arrow)

# Define columns to keep
measurement_cols <- c(
  "FORMATTED DATE_TIME", # Required for date parsing
  "Temperature", "Wet Bulb Temp", "Globe Temperature", "Relative Humidity",
  "Barometric Pressure", "Altitude", "Station Pressure", "Wind Speed",
  "Heat Index", "Dew Point", "Density Altitude", "Crosswind", "Headwind",
  "Compass Magnetic Direction", "NWB Temp", "Compass True Direction",
  "Thermal Work Limit", "Wet Bulb Globe Temperature", "Wind Chill"
)

# Function to parse dates with multiple formats
parse_date <- function(date_str) {
  if (!is.character(date_str) || is.na(date_str) || date_str == "") return(NA)
  
  formats <- c(
    "%Y-%m-%d %I:%M:%S %p",  # 2024-07-09 11:30:12 AM
    "%m/%d/%Y %H:%M",        # 5/24/2024 13:00
    "%m/%d/%y %H:%M",        # 5/24/24 13:00
    "%d/%m/%y %H:%M",        # 24/5/24 13:00
    "%m/%d/%Y %H:%M",        # 5/24/2024 13:00
    "%m/%d/%y %H:%M",        # 5/24/24 13:00
    "%d/%m/%y %H:%M",        # 24/5/24 13:00
    "%Y/%m/%d %H:%M"         # 2024/05/24 13:00
  )
  
  for (fmt in formats) {
    result <- tryCatch(
      as.POSIXct(date_str, format = fmt, tz = "UTC"),
      error = function(e) NA
    )
    if (!is.na(result)) return(result)
  }
  return(NA)
}

# Function to parse and clean a single CSV file
process_csv <- function(file_path) {
  message("Processing file: ", file_path)
  
  # Read CSV, skipping metadata (rows 1-3), using row 4 as header, skipping row 5
  raw_data <- read_csv(
    file_path,
    skip = 3, # Skip metadata
    col_names = TRUE, # Row 4 as header
    n_max = Inf,
    col_types = cols(.default = "c")
  )[-1, ] # Remove row 5 (SI units)
  
  # Check if FORMATTED DATE_TIME exists
  if (!"FORMATTED DATE_TIME" %in% colnames(raw_data)) {
    message("Error: 'FORMATTED DATE_TIME' column not found in ", file_path)
    message("Available columns: ", paste(colnames(raw_data), collapse = ", "))
    return(NULL)
  }
  
  # Select required columns
  available_cols <- intersect(colnames(raw_data), measurement_cols)
  if (length(available_cols) == 0) {
    message("Error: No required columns found in ", file_path)
    return(NULL)
  }
  
  raw_data <- raw_data %>% select(all_of(available_cols))
  
  # Inspect first few date values
  message("Sample FORMATTED DATE_TIME values: ",
          paste(head(raw_data$`FORMATTED DATE_TIME`, 5), collapse = "; "))
  
  # Process data
  processed_data <- raw_data %>%
    mutate(
      raw_date = `FORMATTED DATE_TIME`, # Keep raw for debugging
      date = map_vec(`FORMATTED DATE_TIME`, parse_date), # Apply custom parsing
      across(all_of(setdiff(available_cols, "FORMATTED DATE_TIME")), as.numeric), # Convert measurements to numeric
      file_source = basename(file_path)
    ) %>%
    filter(!is.na(date), !is.na(Temperature)) %>%
    select(
      date, all_of(setdiff(available_cols, "FORMATTED DATE_TIME")), file_source, raw_date
    )
  
  if (nrow(processed_data) == 0) {
    message("No valid rows after parsing in ", file_path)
  }
  
  return(processed_data)
}

# Function to process all CSVs for a single device
process_device <- function(device_id, input_base_dir, output_dir) {
  input_dir <- file.path(input_base_dir, device_id)
  
  # List CSV files
  csv_files <- list.files(
    path = input_dir,
    pattern = "\\.csv$",
    full.names = TRUE
  )
  
  if (length(csv_files) == 0) {
    message("No CSV files found for device ", device_id, " in ", input_dir)
    return(NULL)
  }
  
  message("Found ", length(csv_files), " CSV files for device ", device_id)
  
  # Read and combine CSVs
  combined_data <- map_dfr(csv_files, ~{
    tryCatch(
      process_csv(.x),
      error = function(e) {
        message("Error processing ", .x, ": ", e$message)
        return(NULL)
      }
    )
  })
  
  if (nrow(combined_data) == 0) {
    message("No valid data processed for device ", device_id)
    return(NULL)
  }
  
  # Arrange by date
  combined_data <- combined_data %>%
    arrange(as.POSIXct(date))
  
  # Ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Save to Parquet
  output_parquet <- file.path(output_dir, paste0(device_id, "_combined.parquet"))
  write_parquet(combined_data, output_parquet)
  message("Parquet file saved for ", device_id, ": ", output_parquet)
  
  # Return data and summary
  list(
    data = combined_data,
    summary = combined_data %>%
      summarise(
        min_date = min(date, na.rm = TRUE),
        max_date = max(date, na.rm = TRUE),
        n_rows = n(),
        n_files = n_distinct(file_source)
      )
  )
}

# Main function to process multiple devices
process_all_devices <- function(device_ids, input_base_dir, output_dir) {
  results <- map(device_ids, ~{
    message("\nProcessing device: ", .x)
    process_device(.x, input_base_dir, output_dir)
  }) %>% set_names(device_ids)
  
  # Print summaries
  message("\nProcessing Summary:")
  for (device_id in device_ids) {
    if (!is.null(results[[device_id]])) {
      message("Device: ", device_id)
      print(results[[device_id]]$summary)
    } else {
      message("Device: ", device_id, " - No data processed")
    }
  }
  
  return(results)
}

# Parameters
device_ids <- c("D2924183_1", "D2926895_1", "D2926910_1")
input_base_dir <- "C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/Kestrel1"
output_dir <- "C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog1"

# Run the processing
# results <- process_all_devices(device_ids, input_base_dir, output_dir)
# results
# # Check for problematic dates
# for (device_id in device_ids) {
#   if (!is.null(results[[device_id]])) {
#     result <- results[[device_id]]$data
#     if (any(is.na(result$date))) {
#       message("\nSome dates failed to parse for ", device_id, ". Sample raw dates:")
#       print(head(result$raw_date[is.na(result$date)], 10))
#     }
#   }
# }
# 
# # Display warnings if any
# if (length(warnings()) > 0) {
#   message("\nWarnings encountered:")
#   print(warnings())
# }


# Define parameters
device_ids <- c("D2924183_1", "D2926895_1", "D2926910_1")
output_dir <- "C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog1"

# Function to verify a single Parquet file
verify_parquet <- function(device_id, output_dir) {
  # Construct Parquet file path
  parquet_path <- file.path(output_dir, paste0(device_id, "_combined.parquet"))
  
  # Check if file exists
  if (!file.exists(parquet_path)) {
    message("Parquet file not found for device ", device_id, ": ", parquet_path)
    return(NULL)
  }
  
  # Read Parquet file
  message("Reading Parquet file for device ", device_id, ": ", parquet_path)
  parquet_data <- tryCatch(
    read_parquet(parquet_path),
    error = function(e) {
      message("Error reading ", parquet_path, ": ", e$message)
      return(NULL)
    }
  )
  
  if (is.null(parquet_data)) {
    return(NULL)
  }
  
  # Print summary
  message("\nSummary for device ", device_id, ":")
  print(summary(parquet_data))
  
  # Print yearly row counts
  message("\nYearly row counts for device ", device_id, ":")
  print(table(format(parquet_data$date, "%Y")))
  
  # Return data for further inspection if needed
  parquet_data
}

# Verify all devices
results <- lapply(device_ids, function(device_id) {
  message("\n=== Verifying device: ", device_id, " ===")
  verify_parquet(device_id, output_dir)
}) %>% set_names(device_ids)

# Check for warnings
if (length(warnings()) > 0) {
  message("\nWarnings encountered:")
  print(warnings())
}
