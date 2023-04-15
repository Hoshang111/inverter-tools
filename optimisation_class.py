import pandas as pd

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