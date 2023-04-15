# Databricks notebook source
# DBTITLE 1,Scenario, Optimisation and Assumption Classes
import pandas as pd

class Scenario:
    """
    A class to represent a given Scenario.

    ...

    Attributes
    ----------
    name : str
        name of the scenario
    start : str
        start time of the scenario
    end : str
        end time of the scenario
    optmisation : object
        optimisation table given from scenario, start and end time
    assumption : object
        assumption table given from scenario, start and end time

    Methods
    -------
    None
    """
    def __init__(self, name, start=None, end=None):
        '''
        Initialises Scenario

        Args:
            name (str) : name of the scenario
            start (str) : start time of the scenario
            end (str) : end time of the scenario

        Returns:
            None
        '''
        self.name = name
        self.start = start
        self.end = end
        self.optimisation = Optimisation(self.name, self.start, self.end)
        self.assumption = Assumption(self.name, self.start, self.end)

class Optimisation:
    """
    A class to represent a given scenarios' optimisation table.

    ...

    Attributes
    ----------
    name : str
        name of the optimisation scenario
    start : str
        start time of the queried optimisation table
    end : str
        end time of the queried optimisation table

    Methods
    -------
    create_optimisation_df():
        Creates an optimisation table for the scenario
    """
    def __init__(self, scenario, start, end):
        '''
        Initialises Optimisation Class

        Args:
            name (str) : name of the optimised scenario
            start (str) : start time of the optimised scenario
            end (str) : end time of the optimised scenario

        Returns:
            None
        '''
        self.scenario = scenario
        self.start = start
        self.end = end
        self.df = self.create_optimisation_df()

    def create_optimisation_df(self):
        '''
        Creates Pandas Dataframe for optimsation table

        Args:
            None

        Returns:
            df_optimsations (pandas dataframe object) : Dataframe that gives optimsation of scenario
        '''
        optimisations_query = "select period_end_utc, identifier, value from hive_metastore.federated_postgres.federated_timezoned_optimisations where scenario = '" + self.scenario + "'"
        df_optimisations = spark.sql(optimisations_query).toPandas()
        df_optimisations = df_optimisations.set_index(pd.DatetimeIndex(df_optimisations['period_end_utc']))
        df_optimisations = df_optimisations.pivot(index='period_end_utc', columns='identifier', values='value')
        if self.start != None or self.end != None:
            df_optimisations = df_optimisations[self.start:self.end]    
        return df_optimisations

class Assumption:
    """
    A class to represent a given scenarios' assumption table.

    ...

    Attributes
    ----------
    name : str
        name of the scenario
    start : str
        start time of the queried assumption table
    end : str
        end time of the queried assumption table

    Methods
    -------
    create_assumption_df():
        Creates an assumption table for the scenario
    """
    def __init__(self, scenario, start, end):
        '''
        Initialises Assumption Class

        Args:
            name (str) : name of the assumption scenario
            start (str) : start time of the assumption scenario
            end (str) : end time of the assumption scenario

        Returns:
            None
        '''
        self.scenario = scenario
        self.start = start
        self.end = end
        self.df = self.create_assumption_df()

    def create_assumption_df(self):
        '''
        Creates Pandas Dataframe for assumption table

        Args:
            None

        Returns:
            df_assumptions (pandas dataframe object) : Dataframe that gives assumptions of scenario
        '''
        scenario_assumptions_query = "select * from hive_metastore.federated_postgres.federated_optimisations_assumptions where scenario = '" + self.scenario + "' order by period_start asc"
        df_assumptions = spark.sql(scenario_assumptions_query).toPandas()
        df_assumptions['period_start']= pd.to_datetime(df_assumptions['period_start'])
        df_assumptions['period_end']= pd.to_datetime(df_assumptions['period_end'])
        if self.start != None or self.end != None:
            df_assumptions = df_assumptions.loc[(df_assumptions['period_start'] >= self.start) & (df_assumptions['period_end'] <= self.end)]  
        return df_assumptions


# COMMAND ----------

# DBTITLE 1,Optimisation Test Cases
'''
Test 1: Full Optimisation Table
'''
def test_full_optimisation_table():
    scenario = Scenario("ES21_SAT_1P")
    assert(str(scenario.optimisation.df.index[0]) == '2025-01-01 01:00:00')
    assert(str(scenario.optimisation.df.index[-1]) == '2066-01-01 00:00:00') 
'''
Test 2: Optimisation Table with start time and end time
'''
def test_optimisation_table_with_start_and_end():
    scenario = Scenario("ES21_SAT_1P", start='2030-01-01 01:00:00', end='2035-12-31 20:00:00')
    assert(str(scenario.optimisation.df.index[0]) == '2030-01-01 01:00:00')
    assert(str(scenario.optimisation.df.index[-1]) == '2035-12-31 20:00:00')

'''
Test 3: Optimisation Table with just start time
'''
def test_optimisation_table_with_start():
    scenario = Scenario("ES21_SAT_1P", start='2030-01-01 01:00:00')
    assert(str(scenario.optimisation.df.index[0]) == '2030-01-01 01:00:00')
    assert(str(scenario.optimisation.df.index[-1]) == '2066-01-01 00:00:00')  

'''
Test 4: Optimisation Table with just end time
'''
def test_optimisation_table_with_end():
    scenario = Scenario("ES21_SAT_1P", end='2040-01-01 01:00:00')
    assert(str(scenario.optimisation.df.index[0]) == '2025-01-01 01:00:00')
    assert(str(scenario.optimisation.df.index[-1]) == '2040-01-01 01:00:00')
'''
Uncomment tests which you want to run
'''
if __name == '__main__'
    # test_full_optimisation_table()
    # test_optimisation_table_with_start_and_end()
    # test_optimisation_table_with_start()
    # test_optimisation_table_with_end()

# COMMAND ----------

# DBTITLE 1,Assumption Test Cases
'''
Assumption Test Cases
'''

'''
Test 1: Full Assumption Table
'''
def test_full_assumption_table():
    scenario = Scenario("ES21_SAT_1P")
    assert(str(scenario.assumption.df['period_start'].iat[0]) == '2025-01-01 00:00:00')
    assert(str(scenario.assumption.df['period_end'].iat[-1]) == '2066-01-01 00:00:00') 

'''
Test 2: Assumption Table with start time and end time
'''
def test_assumption_table_with_start_and_end():
    scenario = Scenario("ES21_SAT_1P", start='2030-01-01 01:00:00', end='2035-12-31 20:00:00')
    assert(str(scenario.assumption.df['period_start'].iat[0]) == '2030-01-01 01:00:00')
    assert(str(scenario.assumption.df['period_end'].iat[-1]) == '2035-12-31 20:00:00')

'''
Test 3: Assumption Table with just start time
'''
def test_assumption_table_with_start():
    scenario = Scenario("ES21_SAT_1P", start='2030-01-01 01:00:00')
    assert(str(scenario.assumption.df['period_start'].iat[0]) == '2030-01-01 01:00:00')
    assert(str(scenario.assumption.df['period_end'].iat[-1]) == '2066-01-01 00:00:00')  

'''
Test 4: Assumption Table with just end time
'''
def test_assumption_table_with_end():
    scenario = Scenario("ES21_SAT_1P", end='2040-01-01 01:00:00')
    assert(str(scenario.assumption.df['period_start'].iat[0]) == '2025-01-01 01:00:00')
    assert(str(scenario.assumption.df['period_end'].iat[-1]) == '2040-01-01 01:00:00')

'''
Uncomment tests which you want to run
'''
if __name == '__main__'
    # test_full_assumption_table()
    # test_assumption_table_with_start_and_end()
    # test_assumption_table_with_start()
    # test_assumption_table_with_end()

# COMMAND ----------

# DBTITLE 1,SCOPTI UI
from IPython.display import display
from datetime import datetime
import ipywidgets as widgets

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


def scopti_databricks_ui(is_active):
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
