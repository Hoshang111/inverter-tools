from IPython.display import display
from datetime import datetime
import ipywidgets as widgets
from scenario_class import Scenario
from databricks.sdk.runtime import spark

def get_all_scenarios():
    '''
    Gets all scenarios from the details table

    Args:
        None

    Returns:
            df_scenarios (pandas dataframe object): Dataframe that gives result of all scenarios
    '''
    scenario_details_query = "select * from hive_metastore.federated_postgres.federated_optimisations_scenario_details"
    df_scenarios = spark.sql(scenario_details_query).toPandas()
    return df_scenarios

def get_active_scenarios():
    '''
    Gets the active scenarios from the details table

    Args:
        None
        

    Returns:
            df_scenarios (pandas dataframe object): Dataframe that gives result of active scenarios
    '''
    scenario_details_query = "select * from hive_metastore.federated_postgres.federated_optimisations_scenario_details where is_active == true"
    df_scenarios = spark.sql(scenario_details_query).toPandas()
    return df_scenarios


def scopti_databricks_ui(is_active=True):
    '''
    Creates Databricks UI widgets for a Databricks notebook.

    Args:
        is_active (boolean) : Tells UI whether or not to select active scenarios

    Returns:
            None: This function displays the created widgets.
    '''

    # List of hours in a day in form '00:00:00'
    hours = ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00', '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00', 
    '12:00:00', '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00', '18:00:00', '19:00:00', '20:00:00', '21:00:00', '22:00:00', '23:00:00', '24:00:00']

    # Creates widgets for start and end time parameters
    start_hour = widgets.SelectionSlider(options=hours,value=hours[0], description='Start Hour',
    disabled=False, continuous_update=False, orientation='horizontal', readout=True)
    start_time = widgets.DatePicker(description='Start Time', disabled=False)

    end_hour = widgets.SelectionSlider(options=hours,value=hours[0], description='End Hour',
    disabled=False, continuous_update=False, orientation='horizontal', readout=True)
    end_time = widgets.DatePicker(description='End Time', disabled=False)

    # Creates Scenario dropdown depending on active scenarios are required
    if is_active == True:
        scenarios = get_active_scenarios()
    elif is_active == False:
        scenarios = get_all_scenarios()
    scenario_widget = widgets.Dropdown(options=scenarios['scenario'], description='Scenario')  

    # Create button widget. Clicking these buttons loads a sampled dataframe from queried table.
    optimisation_button = widgets.Button(description="Load Optimisations")
    assumption_button = widgets.Button(description="Load Assumptions")
    pivot_button = widgets.Button(description="Unique Identifiers")

    # Output widget to display the loaded dataframes
    output = widgets.Output()

    def on_optimisation_button_clicked(_):
        '''
        Handles optmisation button click and displays pandas dataframe

        Args:
            None

        Returns:
                None: This function displays the optimisation dataframe
        '''
        with output:
            output.clear_output()
            name = scenario_widget.value
            start_string = start_time.value.strftime("%Y-%m-%d") + ' ' + start_hour.value
            end_string = end_time.value.strftime("%Y-%m-%d") + ' ' + end_hour.value
            scenario = Scenario(name, start_string, end_string)
            display(scenario.optimisation.df)

    def on_assumption_button_clicked(_):
        '''
        Handles assumption button click and displays pandas dataframe

        Args:
            None

        Returns:
                None: This function displays the assumption dataframe
        '''
        with output:
            output.clear_output()
            name = scenario_widget.value
            start_string = start_time.value.strftime("%Y-%m-%d") + ' ' + start_hour.value
            end_string = end_time.value.strftime("%Y-%m-%d") + ' ' + end_hour.value
            scenario = Scenario(name, start_string, end_string)
            display(scenario.assumption.df)

    def on_pivot_button_clicked(_):
        '''
        Handles assumption button click and displays pandas dataframe

        Args:
            None

        Returns:
                None: This function displays the assumption dataframe
        '''
        with output:
            output.clear_output()
            name = scenario_widget.value
            start_string = start_time.value.strftime("%Y-%m-%d") + ' ' + start_hour.value
            end_string = end_time.value.strftime("%Y-%m-%d") + ' ' + end_hour.value
            scenario = Scenario(name, start_string, end_string)
            df = scenario.assumption.df.drop_duplicates(subset=['identifier'])[['identifier','unit','description']]
            display(df)

    # Register the button's callback function to query df and display results to the output widget
    optimisation_button.on_click(on_optimisation_button_clicked)
    assumption_button.on_click(on_optimisation_button_clicked)
    pivot_button.on_click(on_pivot_button_clicked)

    # Collect widgets in boxes
    start_box = widgets.HBox([start_time, start_hour])
    end_box = widgets.HBox([end_time, end_hour])
    buttons_box = widgets.HBox([optimisation_button, assumption_button, pivot_button])
    full_box = widgets.VBox([start_box, end_box, buttons_box])

    # Display the widgets and output
    display(scenario_widget, full_box, output)

if __name__ == "__main__":
    scopti_databricks_ui(True)