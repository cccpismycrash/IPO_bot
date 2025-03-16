import pandas as pd
import warnings
from loguru import logger

class DataValidator:

    def __init__(self) -> None:
        pass

    def _base_validator(self, 
                        _df: pd.DataFrame, 
                        _format: str | None
                        ) -> pd.DataFrame:
        
        _df['date'] = pd.to_datetime(_df['date'], format=_format)
        _df.loc[:, 'Year'] = _df.loc[:, 'date'].dt.year
        _df.loc[:, 'Month'] = _df.loc[:, 'date'].dt.month_name()
        _df = _df[['Month', 'Year', 'company']].groupby(['Month', 'Year']).count()
        _df.reset_index(inplace=True)
        _df.rename(columns={'company': 'Quantity'}, inplace=True)
        _df['sorting'] = pd.to_datetime(_df['Month'] + ' ' + _df['Year'].astype(str))
        _df.sort_values(by=['sorting'], inplace=True, ascending=False)
        _df.reset_index(inplace=True, drop=True)
        _df.drop('sorting', axis=1, inplace=True)
        cols = ['Year', 'Month', 'Quantity']
        _df = _df[cols]

        return _df

    def euronext_validator(self, 
                           df: pd.DataFrame
                           ) -> pd.DataFrame:
        
        logger.info(f'START: Validate Euronext data.')
        
        warnings.simplefilter('ignore')
        cols = ['Date', 'Company name']
        df = df[cols]
        df.rename(columns={'Date': 'date', 'Company name': 'company'}, inplace=True)

        logger.info(f'END: Validate Euronext data.')

        return self._base_validator(_df=df, _format='%d/%m/%Y')

    def preqveca_validator(self, 
                           df: pd.DataFrame
                           ) -> pd.DataFrame:
        
        logger.info(f'START: Validate Preqveca data.')

        warnings.simplefilter('ignore')
        cols = ['Дата окончания размещения', 'Название IPO / SPO']
        df = df[cols]
        df.rename(columns={'Дата окончания размещения': 'date', 'Название IPO / SPO': 'company'}, inplace=True)

        logger.info(f'END: Validate Preqveca data.')

        return self._base_validator(_df=df, _format='%d.%m.%Y')

    def investings_validator(self, 
                             df: pd.DataFrame
                             ) -> pd.DataFrame:
        
        logger.info(f'START: Validate Investings data.')

        warnings.simplefilter('ignore')
        cols = ['Дата IPO', 'Компания']
        df = df[cols]
        df.rename(columns={'Дата IPO': 'date', 'Компания': 'company'}, inplace=True)

        logger.info(f'END: Validate Investings data.')

        return self._base_validator(_df=df, _format=None)
    
    def stockanalysis_validator(self, 
                                df: pd.DataFrame
                                ) -> pd.DataFrame:
        
        logger.info(f'START: Validate Stockanalysis data.')

        warnings.simplefilter('ignore')
        cols = ['IPO Date', 'Company Name']
        df = df[cols]
        df.rename(columns={'IPO Date': 'date', 'Company Name': 'company'}, inplace=True)

        logger.info(f'END: Validate Stockanalysis data.')

        return self._base_validator(_df=df, _format='%b %d, %Y')