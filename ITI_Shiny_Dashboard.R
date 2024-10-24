library(tidyverse)
library(DT)
library(shiny)
library(purrr)
library(ggplot2)
library(plotly)
library(dplyr)

# Load dataset
# Used rds file to store cleaned up version of file
ncvt <- readRDS(gzcon(url("https://uwmadison.box.com/shared/static/1u4m75m8mssrs0ncrqa3s8c2o9a5lirg.rds")))

# Make note of columns 
columns <- colnames(ncvt)
excluded_columns <- c("FatherGuardianName", "MotherName", "TraineeName", "Trainee Reg No")

# Function to create dropdowns for each selected column
create_dropdowns <- function(selected_cols, unique_values, filters) {
    
    lapply(selected_cols, function(col) {
      
      # Add "All" option for all columns in addition to the unique values present in the column
      choices <- c("All", unique_values[[col]])  
      
      # Create select input for each column
      selectInput(inputId = paste0("filter_", col),
                  label = paste("Select value for", col),
                  choices = choices,
                  selected = "All", 
                  multiple = TRUE)
    })
  
}

# Function to filter the data based on the user-selected dropdown filters
filter_data <- function(data, selected_cols, filters) {
  
    for (col in selected_cols) {
      
      filter_value <- filters[[col]] 
      
      # If "All" is not selected and there is no null value for the filter, apply the filter to the data
      if (!is.null(filter_value) && !"All" %in% filter_value) {
        data <- data[data[[col]] %in% filter_value, , drop = FALSE]
      }
      
    }
    
    # Return the filtered dataset with only selected columns
    data[, selected_cols, drop = FALSE]

}

ui <- fluidPage(
  
    titlePanel("Trainee Qualification and Demographics Dashboard"),
    
    # Apply a css stylesheet
    tags$head(tags$link(rel = "stylesheet", type = "text/css", href = "https://uwmadison.box.com/shared/static/o8wlb1nvn153plg2zkdlr3wfap7ans9l.css?raw=1")),
    
    # Used div in order to apply css styling
    
    div(id = "main-container",
        
        div(id = "dropdown-container",
            
              # Italicized descriptive info at the top of the dropdown list
              tags$em("Please select the columns and filters below to alter the dataset for the visualizations. Due to size of dataset, it may take time for plots to render."),
              
              # Dropdown for selecting columns
              selectInput("selected_cols", 
                          label = "Select Columns", 
                          choices = setdiff(columns, excluded_columns),
                          selected = setdiff(columns, excluded_columns),
                          multiple = TRUE),
              
              # Dynamically generated dropdowns for each column filter -- As columns are selected, dropdowns for each of them are created
              uiOutput("dynamic_dropdowns")
        ),
        
        div(id = "plot-and-table-container",
            
              # Time series output
              div(id = "plot-container", 
                  
                  plotlyOutput("timeSeries"),
                  # Dropdown to select column for counting in time series
                  selectInput("time_series_count_col", "Select Column", choices = NULL)
              ), 
              
              # 3d chart output
              div(id = "plot-container", 
                  
                    plotlyOutput("bubbleChart"),
                    # Dropdown to select column for 3D bubble chart
                    selectInput("bubble_3d_col", "Select Column", choices = NULL),
                    tags$em("Bubble size corresponds to percentage of selected dataset each category takes up")
                  
              ),
            
              # Bar chart output
              div(id = "plot-container", 
                  
                    plotlyOutput("barChart"),
                    # Make chart dropdown options be next to each other
                    fluidRow(
                        column(6,  
                               selectInput("bar_x", "Select Column", choices = NULL)
                        ),
                        column(6,  
                               selectInput("bar_y", "Select Color Coding", choices = NULL)
                        )
                    )
                  
              ),  
              
              # Bar chart output
              div(id = "table-container",
                  
                    DT::dataTableOutput("filtered_table")
                  
              )
        )
    )
)

server <- function(input, output, session) {
  
      unique_values <- lapply(ncvt, unique)  # Get unique values for each column in the dataset
      filters <- reactiveValues() # Stores the filters the user selects
      
      # Dynamically render dropdowns for each selected column
      output$dynamic_dropdowns <- renderUI({
        
          req(input$selected_cols)  # Ensure columns are selected
          create_dropdowns(input$selected_cols, unique_values, filters)  # Call the function to create the dropdown menus
          
      })
     
      # Observe changes in each dynamically created dropdown and store the filter values
      observeEvent(input$selected_cols, {
        
          lapply(input$selected_cols, function(col) {
            
              observeEvent(input[[paste0("filter_", col)]], {
                filters[[col]] <- input[[paste0("filter_", col)]]  # Store selected filter values
              }, ignoreInit = TRUE)  # Trigger only when user interacts
            
          })
      
          # Update the dropdown options for the graphs
          # Make sure that even if columns get deleted, there are no errors in plots
          updateSelectInput(session, "bar_x", choices = input$selected_cols, 
                            selected = ifelse("Trade" %in% input$selected_cols, "Trade", input$selected_cols[1])
          )
          updateSelectInput(session, "bar_y", choices = input$selected_cols, 
                            selected = ifelse("Gender" %in% input$selected_cols && "Trade" != "Gender", "Gender", 
                                              ifelse(length(input$selected_cols) > 1, input$selected_cols[2], input$selected_cols[1]))
          )
          updateSelectInput(session, "bubble_3d_col", choices = input$selected_cols, 
                            selected = ifelse("Trade" %in% input$selected_cols, "Trade", input$selected_cols[1])
          )
          updateSelectInput(session, "time_series_count_col", choices = setdiff(input$selected_cols, "Year"), 
                            selected = ifelse("Gender" %in% input$selected_cols, "Gender", 
                                       ifelse("Year" %in% input$selected_cols, input$selected_cols[1], "Gender"))
          )
    
      })
      
      # Filter data reactively -- as the user creates changes
      filtered_data <- reactive({
        
          req(input$selected_cols)
          filter_data(ncvt, input$selected_cols, filters)  # Filter the dataset based on user input
          
      })
      
      # Reactively count trainees for the selected column grouped by year -- this is for the dotplot
      trainee_count_data <- reactive({
        
          req(input$time_series_count_col)
          
          # Aggregate count of trainees for each year and the selected column
          filtered_data() %>%
            group_by(Year, !!sym(input$time_series_count_col)) %>%
            summarise(trainee_count = n()) %>%
            rename(category = !!sym(input$time_series_count_col))  # Rename for clarity
          
      })
      
      # Bar chart output
      output$barChart <- renderPlotly({
        
            req(input$bar_x, input$bar_y)
            
            # Check if input$bar_x and input$bar_y are the same
            # If they are, show an informative message to the user
            validate(
              need(input$bar_x != input$bar_y, "Please change your selection: Column and Color Selection Must Not Equal Each Other")
            )
          
            # Aggregate data to create the bar chart
            bar_data <- filtered_data() %>%
              count(!!sym(input$bar_x), !!sym(input$bar_y)) %>% # Creates column n
              rename(x_var = !!sym(input$bar_x), fill_var = !!sym(input$bar_y))  # Rename for clarity: bar_x and bar_y become x_var and fill_var
            
            # Create the bar chart using plotly
            plot_ly(
              bar_data,
              x = ~n,
              y = ~x_var,
              color = ~fill_var,
              type = 'bar'
            ) %>%
              layout(
                title = paste("Distribution of", input$bar_x, "Colored By", input$bar_y),
                xaxis = list(title = "Count"),
                yaxis = list(title = input$bar_x),
                barmode = 'stack'
              )
            
      })
      
      # 3d bubble plot ouput
      output$bubbleChart <- renderPlotly({
        
          req(input$bubble_3d_col)
          
          # Aggregate data by selected column
          bubble_data <- filtered_data() %>%
            count(!!sym(input$bubble_3d_col)) %>% # Creates column n -- contains counts for each unique value
            rename(category = !!sym(input$bubble_3d_col))  # Rename for clarity in plotly
          
          # Calculate percentages -- for hovering over each bubble
          total_count <- sum(bubble_data$n)
          bubble_data <- bubble_data %>%
            mutate(percentage = (n / total_count) * 100)  # Calculate percentage
          
          # Create the 3D bubble chart using plotly
          plot_ly(
              bubble_data,
              x = ~rnorm(n),  # Random x coordinates 
              y = ~rnorm(n),  # Random y coordinates
              z = ~rnorm(n),  # Random z coordinates 
              size = ~n,  # Size of the bubbles corresponds to the counts
              color = ~category,  # Color by category
              text = ~paste(category, ": ", n, " (", round(percentage, 4), "%)"),  # Create text for category, count, and percentage
              hoverinfo = 'text',  # Display only custom text on hover
              type = 'scatter3d',  # 3D scatter plot
              mode = 'markers',
              marker = list(sizemode = 'diameter', sizeref = 2 * max(bubble_data$n) / (100**2), opacity = 0.6) # Scale the bubble sizes based on the maximum count (n) in the data
          ) %>%
            layout(
                title = paste("Distribution of", input$bubble_3d_col),
                scene = list( # hide axis elements
                  xaxis = list(showticklabels = FALSE, title = ""),  
                  yaxis = list(showticklabels = FALSE, title = ""),  
                  zaxis = list(showticklabels = FALSE, title = "")  
              ),
              showlegend = TRUE 
            )
          
      })
      
      # Time series output / dotplot
      output$timeSeries <- renderPlotly({
        
              # Check if 'Year' is part of the selected columns
              # If not, prompt the user to add it if they would like to see the graph
              validate(
                need('Year' %in% input$selected_cols, "Please change your selection: 'Year' must be in 'Selected Columns' in order to display this plot")
              )
            
              req(trainee_count_data())
              
              # Create time series plot showing count of trainees per selected column grouped by year
              plot_ly(
                trainee_count_data(),
                x = ~Year,
                y = ~trainee_count,
                color = ~category,
                type = 'scatter',
                mode = 'markers',
                size = 20
              ) %>%
                layout(
                  title = paste("Trainee Count by", input$time_series_count_col, "Over Time"),
                  xaxis = list(title = "Year"),
                  yaxis = list(title = "Number of Trainees"),
                  legend = list(title = list(text = input$time_series_count_col))
                )
        
      })
      
      # Render the filtered data table
      output$filtered_table <- DT::renderDataTable({
        
          filtered_data()
        
      }, options = list(pageLength = 10, server = TRUE))
      
}

shinyApp(ui, server)
