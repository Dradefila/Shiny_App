library(shiny)
library(dplyr)
library(DBI)
library(duckdb)
library(DT)
library(shinyjs)
library(shinycssloaders)
library(ggplot2)

ui <- fluidPage(
  useShinyjs(),
  radioButtons("device_id", "Select Device ID:", choices = c("D2924183", "D2926895", "D2926910"), inline = TRUE),
  titlePanel("Environmental Data Explorer"),
  tabsetPanel(
    tabPanel("Summary",
             verbatimTextOutput("summary_text"),
             actionButton("reset_summary", "Reset Summary Table"),
             dataTableOutput("record_summary") %>% withSpinner(color = "#0275d8"),
             br(),
             actionButton("reset_file_list", "Reset File List Table"),
             dataTableOutput("file_list") %>% withSpinner(color = "#0275d8")
    ),
    tabPanel("SQL Query Results",
             fluidRow(
               column(4,
                      uiOutput("sql_examples_ui"),
                      actionButton("load_example", "Insert Example"),
                      br(), br(),
                      actionButton("toggle_csv", "What is a CSV file?"),
                      hidden(
                        div(id = "csv_info",
                            helpText("CSV (Comma-Separated Values) is a simple file format for tabular data, used here for environmental measurements.")
                        )
                      )
               ),
               column(8,
                      textAreaInput("sql", "Enter SQL Query:", "SELECT * FROM env_data LIMIT 100;", rows = 6, width = "100%"),
                      actionButton("run_sql", "Run SQL"),
                      br(), br(),
                      actionButton("reset_sql", "Reset SQL Table")
               )
             ),
             dataTableOutput("sql_results") %>% withSpinner(color = "#0275d8")
    ),
    tabPanel("Visualizations",
             selectInput("plot_type", "Select Plot Type:",
                         choices = c("Temperature Over Time", "Humidity vs. Temperature", "Wind Speed Distribution")),
             plotOutput("env_plot") %>% withSpinner(color = "#0275d8")
    )
  )
)

server <- function(input, output, session) {
  
  summary_trigger <- reactiveVal(0)
  file_data_trigger <- reactiveVal(0)
  sql_data_val <- reactiveVal(data.frame())
  sql_data_trigger <- reactiveVal(0)
  
  db <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  # Load CSV data based on selected device_id
  dataset <- reactive({
    req(input$device_id)
    file_path <- file.path("C:/Users/Hi/OneDrive - University of Oulu and Oamk/Documents/Internship/environmental_data_catalog/",
                           paste0(input$device_id, "_1-Nov-2024_dm.csv"))
    read.csv(file_path, stringsAsFactors = FALSE) %>%
      mutate(`FORMATTED DATE_TIME` = as.Date(`FORMATTED DATE_TIME`, format = "%m/%d/%Y %H:%M"))
  })
  
  df <- reactive({
    dataset() %>%
      rename_with(~ gsub(" ", "_", .x)) # Replace spaces with underscores for SQL compatibility
  })
  
  observeEvent(dataset(), {
    try(duckdb::duckdb_unregister(db, "env_data"), silent = TRUE)
    duckdb::duckdb_register(db, "env_data", df())
  })
  
  # Dynamic prefix for SQL examples
  device_prefix <- reactive({
    input$device_id
  })
  
  # Dynamic list of SQL examples tailored to CSV data
  sql_example_choices <- reactive({
    list(
      "View first 100 rows" = "SELECT * FROM env_data LIMIT 100;",
      "Temperature data" = "SELECT FORMATTED_DATE_TIME, Temperature FROM env_data WHERE Temperature IS NOT NULL;",
      "High wind speed (> 0.5 m/s)" = "SELECT FORMATTED_DATE_TIME, Wind_Speed FROM env_data WHERE Wind_Speed > 0.5;",
      "Average temperature per day" = "
        SELECT FORMATTED_DATE_TIME, ROUND(AVG(Temperature), 2) AS avg_temperature
        FROM env_data
        GROUP BY FORMATTED_DATE_TIME
        ORDER BY FORMATTED_DATE_TIME;",
      "Max altitude per day" = "
        SELECT FORMATTED_DATE_TIME, MAX(Altitude) AS max_altitude
        FROM env_data
        GROUP BY FORMATTED_DATE_TIME;",
      "Humidity and temperature correlation" = "
        SELECT FORMATTED_DATE_TIME, Temperature, Relative_Humidity
        FROM env_data
        WHERE Temperature IS NOT NULL AND Relative_Humidity IS NOT NULL;",
      "Daily summary stats" = "
        SELECT FORMATTED_DATE_TIME,
               ROUND(AVG(Temperature), 2) AS avg_temperature,
               ROUND(AVG(Wind_Speed), 2) AS avg_wind_speed,
               ROUND(AVG(Relative_Humidity), 2) AS avg_humidity
        FROM env_data
        GROUP BY FORMATTED_DATE_TIME
        ORDER BY FORMATTED_DATE_TIME;"
    )
  })
  
  # Dynamic UI for SQL examples
  output$sql_examples_ui <- renderUI({
    selectInput("sql_examples", "SQL Examples:", choices = sql_example_choices())
  })
  
  # Summary Text
  output$summary_text <- renderPrint({
    req(df())
    df() %>%
      summarise(
        total_records = n(),
        date_range = paste(min(FORMATTED_DATE_TIME, na.rm = TRUE), "to", max(FORMATTED_DATE_TIME, na.rm = TRUE)),
        avg_temperature = round(mean(Temperature, na.rm = TRUE), 2),
        avg_humidity = round(mean(Relative_Humidity, na.rm = TRUE), 2)
      )
  })
  
  # Summary Table
  summary_data <- reactive({
    summary_trigger()
    df() %>%
      group_by(FORMATTED_DATE_TIME) %>%
      summarise(
        records = n(),
        avg_temperature = round(mean(Temperature, na.rm = TRUE), 2),
        avg_wind_speed = round(mean(Wind_Speed, na.rm = TRUE), 2),
        avg_humidity = round(mean(Relative_Humidity, na.rm = TRUE), 2),
        .groups = "drop"
      ) %>%
      arrange(FORMATTED_DATE_TIME)
  })
  output$record_summary <- renderDataTable({ summary_data() })
  
  observeEvent(input$reset_summary, {
    summary_trigger(summary_trigger() + 1)
  })
  
  # File List Table
  file_list_data <- reactiveVal()
  
  observeEvent(df(), {
    file_list_data({
      df() %>%
        mutate(
          file_name = paste0(input$device_id, "_1-Nov-2024_dm.csv"),
          date = as.Date(FORMATTED_DATE_TIME)
        ) %>%
        group_by(file_name, date) %>%
        summarise(
          record_count = n(),
          avg_temperature = round(mean(Temperature, na.rm = TRUE), 2),
          avg_wind_speed = round(mean(Wind_Speed, na.rm = TRUE), 2),
          avg_humidity = round(mean(Relative_Humidity, na.rm = TRUE), 2),
          avg_altitude = round(mean(Altitude, na.rm = TRUE), 2),
          .groups = "drop"
        )
    })
  })
  
  output$file_list <- renderDataTable({ file_list_data() })
  observeEvent(input$reset_file_list, { file_list_data(file_list_data()) })
  
  # SQL Results
  sql_result_data <- reactiveVal()
  observeEvent(input$run_sql, {
    tryCatch({
      df <- dbGetQuery(db, input$sql)
      if ("FORMATTED_DATE_TIME" %in% names(df)) {
        df$FORMATTED_DATE_TIME <- as.Date(df$FORMATTED_DATE_TIME)
      }
      sql_result_data(df)
    }, error = function(e) {
      sql_result_data(data.frame(Error = e$message))
    })
  })
  output$sql_results <- renderDataTable({ sql_result_data() })
  observeEvent(input$reset_sql, { sql_result_data(data.frame()) })
  
  observeEvent(input$load_example, {
    updateTextAreaInput(session, "sql", value = input$sql_examples)
  })
  observeEvent(input$toggle_csv, { toggle("csv_info") })
  
  # Visualization
  output$env_plot <- renderPlot({
    req(df())
    plot_data <- df()
    
    if (input$plot_type == "Temperature Over Time") {
      ggplot(plot_data, aes(x = as.Date(FORMATTED_DATE_TIME), y = Temperature)) +
        geom_line(color = "blue") +
        labs(title = "Temperature Over Time", x = "Date", y = "Temperature (°C)") +
        theme_minimal()
    } else if (input$plot_type == "Humidity vs. Temperature") {
      ggplot(plot_data, aes(x = Temperature, y = Relative_Humidity)) +
        geom_point(color = "green") +
        labs(title = "Relative Humidity vs. Temperature", x = "Temperature (°C)", y = "Relative Humidity (%)") +
        theme_minimal()
    } else if (input$plot_type == "Wind Speed Distribution") {
      ggplot(plot_data, aes(x = Wind_Speed)) +
        geom_histogram(fill = "purple", bins = 30) +
        labs(title = "Wind Speed Distribution", x = "Wind Speed (m/s)", y = "Count") +
        theme_minimal()
    }
  })
}

shinyApp(ui, server)
