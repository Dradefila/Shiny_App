# Load required packages
#install.packages("shinyjs")
#install.packages("shiny")
#install.packages("arrow")
#install.packages("dplyr")
#install.packages("shinycssloaders")
#install.packages("ggplot2")
#install.packages("tzdb")
#install.packages(c("DT", "duckdb", "DBI"))
#install.packages(c("arrow", "bslib", "shiny", "memoise", "cachem", "fastmap"))
library(bslib)
library(memoise)
library(cachem)
library(fastmap)
library(DBI)
con <- dbConnect(duckdb::duckdb())
DBI::dbExecute(con, "INSTALL icu")
DBI::dbExecute(con, "LOAD icu")
dbDisconnect(con, shutdown = TRUE)
library(shiny)
library(arrow)
library(tzdb)
library(dplyr)
library(DBI)
library(duckdb)
library(DT)
library(shinyjs)
library(shinycssloaders)
library(ggplot2)

# UI
ui <- fluidPage(
  titlePanel("Environmental Data Viewer"),
  tabsetPanel(
    # Tab 1: Time Series Plot
    tabPanel("Time Series Plot",
             sidebarLayout(
               sidebarPanel(
                 selectInput("device", "Device", choices = c("D2924183", "D2926895", "D2926910")),
                 selectInput("measurement", "Measurement Type",
                             choices = c("Temperature", "Relative Humidity", "Altitude", "Barometric Pressure",
                                         "Dew Point", "Globe Temperature", "Heat Index", "Wind Speed",
                                         "Compass Magnetic Direction", "Compass True Direction", "Crosswind", "Headwind")),
                 dateRangeInput("date_range", "Date Range",
                                start = as.POSIXct("2023-01-01", tz = "UTC"),
                                end = as.POSIXct("2024-07-10", tz = "UTC"))
               ),
               mainPanel(
                 plotOutput("plot")
               )
             )
    ),
    # Tab 2: SQL Query
    tabPanel("SQL Query",
             sidebarLayout(
               sidebarPanel(
                 textAreaInput("sql_query", "Enter SQL Query",
                               value = 'SELECT "FORMATTED DATE_TIME", value, measurement_type, source_file 
                                 FROM dataset 
                                 WHERE device_id = \'D2926910\' 
                                 AND measurement_type = \'Temperature\' 
                                 LIMIT 100',
                               rows = 6),
                 p("Note: Use quotes for column names with spaces, e.g., \"FORMATTED DATE_TIME\"."),
                 actionButton("run_sql", "Run Query")
               ),
               mainPanel(
                 DTOutput("sql_result")
               )
             )
    ),
    # Tab 3: Device Data Table
    tabPanel("Device Data Table",
             sidebarLayout(
               sidebarPanel(
                 selectInput("table_device", "Select Device", choices = c("D2924183", "D2926895", "D2926910"))
               ),
               mainPanel(
                 DTOutput("device_table")
               )
             )
    )
  )
)

# Server
server <- function(input, output, session) {
  # Load Parquet dataset
  ds <- arrow::open_dataset("C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog1")
  
  # Tab 1: Time Series Plot
  output$plot <- renderPlot({
    data <- suppressWarnings(
      ds %>% 
        filter(.data$device_id == input$device,
               .data$measurement_type == input$measurement,
               `FORMATTED DATE_TIME` >= as.POSIXct(input$date_range[1], tz = "UTC"),
               `FORMATTED DATE_TIME` <= as.POSIXct(input$date_range[2], tz = "UTC")) %>% 
        select(`FORMATTED DATE_TIME`, value) %>% 
        collect()
    )
    ggplot(data, aes(x = `FORMATTED DATE_TIME`, y = value)) +
      geom_line() +
      labs(title = paste(input$measurement, "for", input$device),
           x = "Date", y = input$measurement)
  })
  
  # Tab 2: SQL Query
  output$sql_result <- renderDT({
    input$run_sql  # Trigger on button click
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
shinyApp(ui, server)
