# Load required packages
library(shiny)
library(arrow)
library(dplyr)
library(ggplot2)
library(DT)
library(duckdb)
library(DBI)
library(shinyjs)
library(shinycssloaders)

# UI
ui <- fluidPage(
  useShinyjs(), # Initialize shinyjs
  titlePanel("Environmental Data Viewer"),
  tabsetPanel(
    # Tab 1: Time Series Plot
    tabPanel("Time Series Plot",
             sidebarLayout(
               sidebarPanel(
                 selectInput("device", "Device", choices = c("D2924183", "D2926895", "D2926910")),
                 selectInput("measurements", "Measurement Types",
                             choices = c(
                               "Temperature", "Wet Bulb Temp", "Globe Temperature", "Relative Humidity",
                               "Barometric Pressure", "Altitude", "Station Pressure", "Wind Speed",
                               "Heat Index", "Dew Point", "Density Altitude", "Crosswind", "Headwind",
                               "Compass Magnetic Direction", "NWB Temp", "Compass True Direction",
                               "Thermal Work Limit", "Wet Bulb Globe Temperature", "Wind Chill"
                             ),
                             multiple = TRUE,
                             selected = "Temperature"),
                 dateRangeInput("date_range", "Date Range",
                                start = as.POSIXct("2024-07-01", tz = "UTC"),
                                end = as.POSIXct("2024-07-31", tz = "UTC"))
               ),
               mainPanel(
                 withSpinner(plotOutput("plot"))
               )
             )
    ),
    # Tab 2: SQL Query
    tabPanel("SQL Query",
             sidebarLayout(
               sidebarPanel(
                 selectInput("sql_device", "Select Device for SQL Query",
                             choices = c("D2924183", "D2926895", "D2926910")),
                 textAreaInput("sql_query", "Enter SQL Query",
                               value = 'SELECT "FORMATTED DATE_TIME", value, measurement_type, source_file FROM dataset WHERE device_id = \'D2926910\' AND measurement_type = \'Temperature\' AND "FORMATTED DATE_TIME" BETWEEN \'2024-07-01\' AND \'2024-07-31\' LIMIT 100',
                               rows = 6),
                 p("Note: Use quotes for column names with spaces, e.g., \"FORMATTED DATE_TIME\". Use single device queries (e.g., device_id = \'D2926910\')."),
                 actionButton("run_sql", "Run Query")
               ),
               mainPanel(
                 withSpinner(DTOutput("sql_result"))
               )
             )
    ),
    # Tab 3: Device Data Table
    tabPanel("Device Data Table",
             sidebarLayout(
               sidebarPanel(
                 selectInput("table_device", "Select Device",
                             choices = c("D2924183", "D2926895", "D2926910"))
               ),
               mainPanel(
                 withSpinner(DTOutput("device_table"))
               )
             )
    )
  )
)

# Server
server <- function(input, output, session) {
  
  # Load Parquet dataset
  ds <- arrow::open_dataset("C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog_2")
  
  # Update SQL query based on device selection
  observe({
    updateTextAreaInput(session, "sql_query",
                        value = sprintf('SELECT "FORMATTED DATE_TIME", value, measurement_type, source_file FROM dataset WHERE device_id = \'%s\' AND measurement_type = \'Temperature\' AND "FORMATTED DATE_TIME" BETWEEN \'2024-07-01\' AND \'2024-07-31\' LIMIT 100',
                                        input$sql_device))
  })
  
  # Tab 1: Time Series Plot
  output$plot <- renderPlot({
    req(input$measurements) # Ensure at least one measurement is selected
    data <- suppressWarnings(
      ds %>%
        filter(.data$device_id == input$device,
               .data$measurement_type %in% input$measurements,
               `FORMATTED DATE_TIME` >= as.POSIXct(input$date_range[1], tz = "UTC"),
               `FORMATTED DATE_TIME` <= as.POSIXct(input$date_range[2], tz = "UTC")) %>%
        select(`FORMATTED DATE_TIME`, value, measurement_type) %>%
        collect()
    )
    y_label <- if (length(input$measurements) == 1) {
      input$measurements
    } else {
      "Value (Mixed Units)"
    }
    ggplot(data, aes(x = `FORMATTED DATE_TIME`, y = value, color = measurement_type)) +
      geom_line() +
      labs(title = paste("Measurements for", input$device),
           x = "Date",
           y = y_label) +
      theme_minimal() +
      theme(panel.grid.minor = element_line(color = "gray90", linetype = "dashed")) +
      scale_color_discrete(name = "Measurement Type")
  })
  
  # Tab 2: SQL Query
  output$sql_result <- renderDT({
    input$run_sql # Trigger on button click
    isolate({
      req(input$sql_query)
      con <- dbConnect(duckdb::duckdb())
      on.exit(dbDisconnect(con, shutdown = TRUE))
      # Register Parquet dataset as a table
      duckdb::duckdb_register_arrow(con, "dataset", ds)
      # Execute SQL query
      result <- tryCatch(
        {
          suppressWarnings(
            DBI::dbGetQuery(con, input$sql_query)
          )
        },
        error = function(e) {
          data.frame(Error = paste("SQL Error:", e$message))
        }
      )
      datatable(result, options = list(pageLength = 10, autoWidth = TRUE))
    })
  })
  
  # Tab 3: Device Data Table
  output$device_table <- renderDT({
    data <- suppressWarnings(
      ds %>%
        filter(.data$device_id == input$table_device) %>%
        select(`FORMATTED DATE_TIME`, value, measurement_type, source_file) %>%
        collect()
    )
    datatable(data, options = list(pageLength = 10, autoWidth = TRUE))
  })
}

# Run the app
shinyApp(ui = ui, server = server)

