import pandas as pd
import pendulum
from loguru import logger

class DataCalculator:
    def __init__(self):
        pass

    def prepare_month_df(
            self,
            df: pd.DataFrame,
            caption_type: str
            ) -> pd.DataFrame:
        
        logger.info(f'START: Prepare monthly data for IPO in {caption_type} region.')
        
        df = df[df['Year'] == pendulum.now('Europe/Moscow').year]
        df = df[['Month', 'Quantity']].groupby('Month').sum()
        df.reset_index(inplace=True)

        logger.info(f'END: Prepare monthly data for IPO in {caption_type} region.')

        return df

    def prepare_year_df(
            self,
            df: pd.DataFrame,
            caption_type: str
            ) -> pd.DataFrame:
        
        logger.info(f'START: Prepare yearly data for IPO in {caption_type} region.')

        df = df[df['Year'] == pendulum.now('Europe/Moscow').year]
        df = df[['Year', 'Quantity']].groupby('Year').sum()
        df.reset_index(inplace=True)

        logger.info(f'END: Prepare yearly data for IPO in {caption_type} region.')

        return df
