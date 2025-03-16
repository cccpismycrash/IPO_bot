import pandas as pd
from loguru import logger

class DataUpdater:
    def __init__(self) -> None:
        pass

    def update_month_data(self,
            prev_data: pd.DataFrame,
            parsed_data: pd.DataFrame,
            path: str,
            caption_type: str
            ) -> pd.DataFrame:
        
        logger.info(f'START: Update monthly data for IPO in {caption_type} region.')
        
        for i in range(len(parsed_data)):
            row = parsed_data.iloc[i]
            year = row['Year']
            month = row['Month']
            if ((prev_data['Year'] == row['Year']) & (prev_data['Month'] == row['Month'])).any():
                prev_data.loc[(prev_data['Year'] == year) & (prev_data['Month'] == month), ['Quantity']] = row['Quantity']
            else:
                prev_data = pd.concat([prev_data, pd.DataFrame([row])], axis=0, ignore_index=True)
        
        prev_data.to_csv(path, index=False, sep=';')

        logger.info(f'END: Update monthly data for IPO in {caption_type} region.')

        return prev_data
    
    def update_year_data(self,
            prev_data: pd.DataFrame,
            parsed_data: pd.DataFrame,
            path: str, 
            caption_type: str
            ) -> pd.DataFrame:
        
        logger.info(f'START: Update yearly data for IPO in {caption_type} region.')

        for i in range(len(parsed_data)):
            row = parsed_data.iloc[i]
            year = row['Year']
            if (prev_data['Year'] == row['Year']).any():
                prev_data.loc[(prev_data['Year'] == year), ['Quantity']] = row['Quantity']
            else:
                prev_data = pd.concat([prev_data, pd.DataFrame([row])], axis=0, ignore_index=True)

        prev_data.to_csv(path, index=False, sep=';')

        logger.info(f'END: Update yearly data for IPO in {caption_type} region.')

        return prev_data


    