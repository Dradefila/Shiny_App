# Load required packages
library(arrow)
library(dplyr)
library(tidyr)
library(readr)
library(fs)
library(utils)

# Define directories
input_dir <- "C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/Kestrel_2"
output_dir <- "C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog_2"
temp_unzip_dir <- file.path(tempdir(), "unzip_temp")

# Create output and temp directories if they don't exist
dir_create(output_dir, recurse = TRUE)
dir_create(temp_unzip_dir, recurse = TRUE)

# Function to parse dates with multiple formats
parse_date <- function(date_str) {
  # Handle NA, empty, or non-character inputs
  if (!is.character(date_str) || is.na(date_str) || date_str == "") return(NA)
  formats <- c(
    "%Y-%m-%d %I:%M:%S %p", # 2024-07-09 11:30:12 AM
    "%m/%d/%Y %H:%M",       # 5/24/2024 13:00
    "%m/%d/%y %H:%M",       # 5/24/24 13:00
    "%d/%m/%y %H:%M",       # 24/5/24 13:00
    "%m/%d/%Y %H:%M",       # 5/24/2024 13:00 (no space)
    "%m/%d/%y %H:%M",       # 5/24/24 13:00
    "%d/%m/%y %H:%M"        # 24/5/24 13:00
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

# Function to process a single CSV file
process_csv_to_parquet <- function(csv_file, device_id, output_dir) {
  tryCatch({
    # Define expected columns (use spaces for FORMATTED DATE_TIME)
    expected_cols <- c(
      "FORMATTED DATE_TIME", "Temperature", "Wet Bulb Temp", "Globe Temperature", "Relative Humidity",
      "Barometric Pressure", "Altitude", "Station Pressure", "Wind Speed", "Heat Index", "Dew Point",
      "Density Altitude", "Crosswind", "Headwind", "Compass Magnetic Direction", "NWB Temp",
      "Compass True Direction", "Thermal Work Limit", "Wet Bulb Globe Temperature", "Wind Chill",
      "Data Type", "Record name", "Start time", "Duration (H:M:S)", "Location description",
      "Location address", "Location coordinates", "Notes"
    )
    
    # Read the header row (4th row, skip 3 rows)
    header <- read_csv(csv_file, skip = 3, n_max = 1, col_names = TRUE,
                       show_col_types = FALSE, quote = "\"", guess_max = 1000)
    col_names <- names(header)
    message(sprintf("Header columns (%d): %s", length(col_names), paste(col_names, collapse = ", ")))
    
    # Log parsing problems for header
    if (nrow(problems(header)) > 0) {
      message(sprintf("Parsing issues in header of %s:", csv_file))
      print(problems(header))
    }
    
    # Validate header
    if (length(col_names) != length(expected_cols) || !all(col_names %in% expected_cols)) {
      message(sprintf("Unexpected header in %s: found %d columns, expected %d",
                      csv_file, length(col_names), length(expected_cols)))
    }
    
    # Ensure FORMATTED DATE_TIME exists
    if (!"FORMATTED DATE_TIME" %in% col_names) {
      stop(sprintf("FORMATTED DATE_TIME column missing in %s", csv_file))
    }
    
    # Read data starting from 6th row
    data <- read_csv(csv_file, skip = 5, col_names = col_names, show_col_types = FALSE,
                     quote = "\"", guess_max = 1000)
    
    # Log parsing problems for data
    if (nrow(problems(data)) > 0) {
      message(sprintf("Parsing issues in data of %s:", csv_file))
      print(problems(data))
    }
    
    # Check for column mismatch
    if (ncol(data) != length(col_names)) {
      message(sprintf("Column mismatch in %s: expected %d columns, found %d.",
                      csv_file, length(col_names), ncol(data)))
      missing_cols <- setdiff(col_names, names(data))
      message(sprintf("Missing columns: %s", paste(missing_cols, collapse = ", ")))
      for (col in missing_cols) {
        data[[col]] <- NA
      }
      data <- data[, col_names, drop = FALSE]
    }
    
    # Add device_id and source_file
    data$device_id <- device_id
    data$source_file <- basename(csv_file)
    
    # Define measurement columns
    measurement_cols <- c(
      "Temperature", "Wet Bulb Temp", "Globe Temperature", "Relative Humidity",
      "Barometric Pressure", "Altitude", "Station Pressure", "Wind Speed",
      "Heat Index", "Dew Point", "Density Altitude", "Crosswind", "Headwind",
      "Compass Magnetic Direction", "NWB Temp", "Compass True Direction",
      "Thermal Work Limit", "Wet Bulb Globe Temperature", "Wind Chill"
    )
    
    # Keep relevant columns
    keep_cols <- c("FORMATTED DATE_TIME", "device_id", "source_file", measurement_cols)
    data <- data %>% select(any_of(keep_cols))
    
    # Convert measurement columns to numeric
    for (col in measurement_cols) {
      if (col %in% names(data)) {
        data[[col]] <- ifelse(data[[col]] %in% c("--", "", "NA", "N/A"), NA, data[[col]])
        non_numeric <- data[[col]][!is.na(data[[col]]) & !grepl("^-?\\d*\\.?\\d*$", data[[col]])]
        if (length(non_numeric) > 0) {
          message(sprintf("Non-numeric values in %s for column %s: %s",
                          csv_file, col, paste(unique(non_numeric), collapse = ", ")))
        }
        data[[col]] <- as.numeric(data[[col]])
      }
    }
    
    # Check FORMATTED DATE_TIME
    if (!"FORMATTED DATE_TIME" %in% names(data)) {
      stop(sprintf("FORMATTED DATE_TIME column not found in data for %s", csv_file))
    }
    
    # Log sample FORMATTED DATE_TIME values
    sample_dates <- head(data$`FORMATTED DATE_TIME`[!is.na(data$`FORMATTED DATE_TIME`)], 10)
    if (length(sample_dates) > 0) {
      message(sprintf("Sample FORMATTED DATE_TIME values in %s: %s",
                      csv_file, paste(sample_dates, collapse = ", ")))
    }
    
    # Pivot to long format
    data_long <- data %>%
      tidyr::pivot_longer(
        cols = any_of(measurement_cols),
        names_to = "measurement_type",
        values_to = "value"
      )
    
    # Parse dates
    data_long <- data_long %>%
      mutate(
        `FORMATTED DATE_TIME` = sapply(as.character(`FORMATTED DATE_TIME`), parse_date),
        `FORMATTED DATE_TIME` = as.POSIXct(`FORMATTED DATE_TIME`, origin = "1970-01-01", tz = "UTC")
      )
    
    # Log unparseable dates
    invalid_dates <- data$`FORMATTED DATE_TIME`[!is.na(data$`FORMATTED DATE_TIME`) &
                                                  is.na(sapply(as.character(data$`FORMATTED DATE_TIME`), parse_date))]
    if (length(invalid_dates) > 0) {
      message(sprintf("Unparseable dates in %s: %s", csv_file, paste(unique(invalid_dates), collapse = ", ")))
    }
    
    # Filter out NA values
    data_long <- data_long %>% filter(!is.na(value))
    
    message(sprintf("Processed %d rows for %s", nrow(data_long), csv_file))
    
    # Write to Parquet
    write_dataset(
      data_long,
      path = output_dir,
      partitioning = c("device_id", "measurement_type"),
      format = "parquet"
    )
    
    message(sprintf("Successfully processed %s", csv_file))
  }, error = function(e) {
    message(sprintf("Error processing %s: %s", csv_file, e$message))
    # Log first few rows of data
    data_preview <- tryCatch({
      read_csv(csv_file, skip = 5, col_names = col_names, n_max = 5, show_col_types = FALSE, quote = "\"")
    }, error = function(e) NULL)
    if (!is.null(data_preview)) {
      message("Preview of first 5 rows:")
      print(data_preview)
    }
  })
}

# Clear existing Parquet files
if (dir.exists(output_dir)) {
  unlink(file.path(output_dir, "*"), recursive = TRUE)
  message("Cleared existing Parquet files in ", output_dir)
}
dir_create(output_dir, showWarnings = FALSE, recurse = TRUE)

# Process zipped folders
devices <- c("D2924183", "D2926895", "D2926910")
for (device in devices) {
  zip_file <- file.path(input_dir, paste0(device, ".zip"))
  if (!file.exists(zip_file)) {
    message("Zip file not found for device: ", device)
    next
  }
  message("Processing zip file: ", zip_file)
  
  # Create a unique temp directory for this zip
  unzip_dir <- file.path(temp_unzip_dir, device)
  dir_create(unzip_dir, recurse = TRUE)
  
  # Unzip the folder
  unzip(zip_file, exdir = unzip_dir)
  
  # Get list of CSV files in the unzipped folder
  csv_files <- dir_ls(unzip_dir, regexp = "\\.csv$", recurse = TRUE)
  if (length(csv_files) == 0) {
    message("No CSV files found in zip for device: ", device)
    dir_delete(unzip_dir)
    next
  }
  
  # Process each CSV file
  for (csv_file in csv_files) {
    if (file.exists(csv_file)) {
      message("Processing CSV file: ", csv_file)
      # Extract device ID from the zip file name
      process_csv_to_parquet(csv_file, device, output_dir)
    } else {
      message("CSV file not found: ", csv_file)
    }
  }
  
  # Clean up temp directory
  dir_delete(unzip_dir)
}

# Clean up temp directory
if (dir_exists(temp_unzip_dir)) {
  dir_delete(temp_unzip_dir)
}

# Verify the output
tryCatch({
  ds <- open_dataset(output_dir)
  print(ds %>% collect() %>% head())
}, error = function(e) {
  message("Error verifying output: ", e$message)
})
