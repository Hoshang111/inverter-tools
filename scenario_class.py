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

