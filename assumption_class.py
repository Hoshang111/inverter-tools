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
        self.df_num = self.create_assumption_df()
        self.filter_df(self.start, self.end)

    def create_assumption_df(self):
        '''
        Creates Pandas Dataframe for assumption table

        Args:
            None

        Returns:
            df_assumptions_num (pandas dataframe object) : Dataframe that gives assumptions of scenario
        '''
        scenario_assumptions_query = "select * from hive_metastore.federated_postgres.federated_optimisations_assumptions where scenario = '" + self.scenario + "' order by period_start asc"
        df_assumptions = spark.sql(scenario_assumptions_query).toPandas()
        df_assumptions['period_start']= pd.to_datetime(df_assumptions['period_start'])
        df_assumptions['period_end']= pd.to_datetime(df_assumptions['period_end'])

        df_assumptions_num = df_assumptions[df_assumptions['string_value'] = None]
        df_assumptions_str = df_assumptions[df_assumptions['string_value'] != None]

        # Pivot on period_start, fill forward any NA with last good value or 0.
        df_assumptions_num = pd.pivot(index='period_start',columns='identifier', values='numeric_value').fillna(method='ffill').fillna(value=0)

        return df_assumptions_num
    
    def filter_df(self, start, end):
        '''
        Filters the assumptions df based on user gui inputs. Designed to prevent re-querying of the scenario dbs.
        Assigns to self.df.

        Args:
            start (str) : start time of the scenario
            end (str) : end time of the scenario

        Returns:
            None
        '''

        # Filter based on start/end time.
        if start != None or end != None:
            df_filter = self.df_num[start:end]   

        self.df = df_filter
