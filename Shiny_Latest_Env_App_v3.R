library(shiny)
library(ggplot2)
library(arrow)
library(duckdb)
library(dplyr)
library(DT)
library(tidyr)
library(shinyjs)
library(shinycssloaders)

# Define parameters
parquet_dir <- "C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog1"
device_ids <- c("D2924183_1", "D2926895_1", "D2926910_1")
numeric_columns <- c("Temperature", "Wet Bulb Temp", "Globe Temperature", "Relative Humidity",
                     "Barometric Pressure", "Altitude", "Station Pressure", "Wind Speed",
                     "Heat Index", "Dew Point", "Density Altitude", "Crosswind", "Headwind",
                     "Compass Magnetic Direction", "NWB Temp", "Compass True Direction",
                     "Thermal Work Limit", "Wet Bulb Globe Temperature", "Wind Chill")

# UI
ui <- fluidPage(
  useShinyjs(),
  titlePanel("Environmental Data Viewer"),
  tabsetPanel(
    # Tab 1: Time Series Plot
    tabPanel("Time Series Plot",
             sidebarLayout(
               sidebarPanel(
                 selectInput("device_id", "Device ID", choices = device_ids, multiple = TRUE, selected = device_ids[1]),
                 selectInput("measurements", "Measurement Types", choices = numeric_columns,
                             multiple = TRUE, selected = c("Temperature", "Relative Humidity")),
                 dateRangeInput("date_range", "Date Range", start = NULL, end = NULL),
                 actionButton("plot_query", "Run Plot")
               ),
               mainPanel(
                 plotOutput("plot") %>% withSpinner()
               )
             )
    ),
    # Tab 2: SQL Query
    tabPanel("SQL Query",
             sidebarLayout(
               sidebarPanel(
                 selectInput("sql_device", "Select Device for SQL Query", choices = device_ids),
                 textAreaInput("sql_query", "Enter SQL Query",
                               value = 'SELECT DISTINCT date, "Temperature", "Relative Humidity" FROM dataset LIMIT 100',
                               rows = 6),
                 p("Note: Use quotes for column names with spaces, e.g., \"Relative Humidity\". Device ID is inferred from the selected Parquet file."),
                 actionButton("run_sql", "Run Query"),
                 h4("Schema Debug"),
                 verbatimTextOutput("schema_debug")
               ),
               mainPanel(
                 DTOutput("sql_result") %>% withSpinner()
               )
             )
    ),
    # Tab 3: Device Data Table
    tabPanel("Device Data Table",
             sidebarLayout(
               sidebarPanel(
                 selectInput("table_device", "Select Device", choices = device_ids)
               ),
               mainPanel(
                 DTOutput("device_table") %>% withSpinner()
               )
             )
    )
  )
)

# Server
server <- function(input, output, session) {
  # Get date range from Parquet files
  date_range <- reactive({
    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))
    parquet_files <- file.path(parquet_dir, paste0(device_ids, "_combined.parquet"))
    file_exists <- file.exists(parquet_files)
    if (!any(file_exists)) {
      showNotification("No Parquet files found.", type = "error")
      return(NULL)
    }
    dates <- vector("list", length(parquet_files))
    for (i in seq_along(parquet_files)) {
      if (!file_exists[i]) next
      ds <- tryCatch({
        open_dataset(parquet_files[i])
      }, error = function(e) {
        NULL
      })
      if (is.null(ds)) next
      duckdb_register_arrow(con, paste0("temp_", i), ds)
      query <- sprintf("SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM temp_%d", i)
      result <- tryCatch({
        dbGetQuery(con, query)
      }, error = function(e) {
        NULL
      })
      dates[[i]] <- result
    }
    dates <- bind_rows(dates)
    if (nrow(dates) == 0) {
      showNotification("No valid date ranges found.", type = "error")
      return(NULL)
    }
    list(min = as.Date(min(dates$min_date, na.rm = TRUE)),
         max = as.Date(max(dates$max_date, na.rm = TRUE)))
  })
  
  # Update dateRangeInput
  observe({
    dr <- date_range()
    req(dr)
    updateDateRangeInput(session, "date_range",
                         start = dr$min,
                         end = dr$max,
                         min = NULL,
                         max = NULL)
  })
  
  # Schema debug output for SQL tab
  output$schema_debug <- renderText({
    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))
    parquet_file <- file.path(parquet_dir, paste0(input$sql_device, "_combined.parquet"))
    if (!file.exists(parquet_file)) {
      return("Parquet file not found for selected device.")
    }
    ds <- tryCatch({
      open_dataset(parquet_file)
    }, error = function(e) {
      return(sprintf("Error reading Parquet: %s", e$message))
    })
    duckdb_register_arrow(con, "dataset", ds)
    schema <- dbGetQuery(con, "DESCRIBE dataset")
    paste("Parquet Schema for", input$sql_device, ":\n", paste(capture.output(print(schema)), collapse = "\n"))
  })
  
  # Tab 1: Time Series Plot
  output$plot <- renderPlot({
    input$plot_query
    isolate({
      req(input$device_id, input$measurements, input$date_range)
      con <- dbConnect(duckdb::duckdb())
      on.exit(dbDisconnect(con, shutdown = TRUE))
      parquet_files <- file.path(parquet_dir, paste0(input$device_id, "_combined.parquet"))
      file_exists <- file.exists(parquet_files)
      if (!any(file_exists)) {
        showNotification("No Parquet files found for selected devices.", type = "error")
        return(NULL)
      }
      query_parts <- character(length(input$device_id))
      for (i in seq_along(input$device_id)) {
        if (!file_exists[i]) next
        table_name <- paste0("device_", i)
        ds <- tryCatch({
          open_dataset(parquet_files[i])
        }, error = function(e) {
          NULL
        })
        if (is.null(ds)) next
        duckdb_register_arrow(con, table_name, ds)
        query_parts[i] <- sprintf(
          "SELECT DISTINCT '%s' AS device_id, date, %s FROM %s WHERE date >= '%s' AND date <= '%s'",
          input$device_id[i],
          paste(sprintf('"%s"', input$measurements), collapse = ", "),
          table_name,
          as.character(input$date_range[1]),
          as.character(input$date_range[2])
        )
      }
      if (all(is.na(query_parts))) {
        showNotification("No valid Parquet files could be queried.", type = "error")
        return(NULL)
      }
      query <- sprintf(
        "SELECT DISTINCT device_id, date, %s FROM (%s)",
        paste(sprintf('"%s"', input$measurements), collapse = ", "),
        paste(query_parts[!is.na(query_parts)], collapse = " UNION ALL ")
      )
      df <- tryCatch({
        dbGetQuery(con, query)
      }, error = function(e) {
        showNotification(sprintf("Query error: %s", e$message), type = "error")
        return(NULL)
      })
      req(df, nrow(df) > 0)
      data <- df %>%
        pivot_longer(cols = all_of(input$measurements), names_to = "measurement_type", values_to = "value") %>%
        mutate(date = as.POSIXct(date, tz = "UTC"))
      ggplot(data, aes(x = date, y = value, color = measurement_type, linetype = device_id)) +
        geom_line() +
        labs(title = "Environmental Measurements Over Time",
             x = "Date",
             y = "Value") +
        theme_minimal() +
        scale_x_datetime(date_labels = "%Y-%m-%d %H:%M:%S") +
        scale_color_discrete(name = "Measurement Type")
    })
  })
  
  # Tab 2: SQL Query
  output$sql_result <- renderDT({
    input$run_sql
    isolate({
      req(input$sql_query)
      con <- dbConnect(duckdb::duckdb())
      on.exit(dbDisconnect(con, shutdown = TRUE))
      parquet_file <- file.path(parquet_dir, paste0(input$sql_device, "_combined.parquet"))
      if (!file.exists(parquet_file)) {
        showNotification("Parquet file not found.", type = "error")
        return(datatable(data.frame(Error = "No Parquet file found")))
      }
      ds <- tryCatch({
        open_dataset(parquet_file)
      }, error = function(e) {
        showNotification(sprintf("Error reading Parquet: %s", e$message), type = "error")
        return(datatable(data.frame(Error = "Parquet read error")))
      })
      duckdb_register_arrow(con, "dataset", ds)
      result <- tryCatch({
        dbGetQuery(con, input$sql_query)
      }, error = function(e) {
        showNotification(sprintf("SQL Error: %s", e$message), type = "error")
        return(datatable(data.frame(Error = sprintf("SQL Error: %s", e$message))))
      })
      datatable(result, options = list(pageLength = 10, autoWidth = TRUE, scrollX = TRUE))
    })
  })
  
  # Tab 3: Device Data Table
  output$device_table <- renderDT({
    req(input$table_device)
    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE))
    parquet_file <- file.path(parquet_dir, paste0(input$table_device, "_combined.parquet"))
    if (!file.exists(parquet_file)) {
      showNotification("Parquet file not found.", type = "error")
      return(datatable(data.frame(Error = "No Parquet file found")))
    }
    ds <- tryCatch({
      open_dataset(parquet_file)
    }, error = function(e) {
      showNotification(sprintf("Error reading Parquet: %s", e$message), type = "error")
      return(datatable(data.frame(Error = "Parquet read error")))
    })
    query <- sprintf(
      "SELECT DISTINCT '%s' AS device_id, date, %s FROM dataset",
      input$table_device,
      paste(sprintf('"%s"', numeric_columns), collapse = ", ")
    )
    duckdb_register_arrow(con, "dataset", ds)
    data <- tryCatch({
      dbGetQuery(con, query)
    }, error = function(e) {
      showNotification(sprintf("Query error: %s", e$message), type = "error")
      return(datatable(data.frame(Error = sprintf("Query error: %s", e$message))))
    })
    datatable(data, options = list(pageLength = 10, autoWidth = TRUE, scrollX = TRUE))
  })
  
  # Update SQL query based on device selection
  observe({
    updateTextAreaInput(session, "sql_query",
                        value = 'SELECT DISTINCT date, "Temperature", "Relative Humidity" FROM dataset LIMIT 100')
  })
}

# Run the app
shinyApp(ui, server)
