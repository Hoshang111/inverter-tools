import pandas as pd

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
