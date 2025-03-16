from httpx import AsyncClient, Response, QueryParams, Headers
import pandas as pd
import asyncio
import pendulum
from bs4 import BeautifulSoup
from loguru import logger

class InvestingsParser:
    def __init__(self) -> None:
        self._http_client = AsyncClient()

    async def _request(self, url: str, body: QueryParams, headers: Headers) -> Response:
        response = await self._http_client.post(url=url, data=body, headers=headers)
        response.raise_for_status()
        return response
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df['Дата IPO'] = pd.to_datetime(df['Дата IPO'])
        return df

    async def _parse_part_of_data(self, start: str, end: str, country: int = 37) -> pd.DataFrame:

        # today = pendulum.now('Europe/Moscow').date()
        # prev_year = today.subtract(years=1).year
        # start = pendulum.date(year=prev_year, month=1, day=1).format('Y-MM-DD')
        # end = today.format('Y-MM-DD')

        url = 'https://www.investing.com/ipo-calendar/Service/getCalendarFilteredData'

        body_types = {
            'country[]': str(country),  # 5 - это США, 37 - Китай, например
            'dateFrom': str(start),
            'dateTo': str(end),
            'currentTab': 'custom',
            'submitFilters': str(1),
            'limit_from': str(0)
        }

        body = QueryParams(body_types)

        headers = {
            'x-requested-with': 'XMLHttpRequest',
        }

        try:
            response = await self._request(url=url, body=body, headers=headers)
        except Exception as e:
            logger.error(f'Error while parsing Investings')

        res_list = []
        data = response.json()
        soup = BeautifulSoup(data['data'], 'html.parser')
        rows = soup.findAll('tr')
        for row in rows:
            row_columns = row.find_all('td')

            date_ipo = row_columns[0].text
            company_name = row_columns[1].find('span', class_='elp').text
            exchange_name = row_columns[2].text
            estimation_ipo = row_columns[3].text
            price_ipo = row_columns[4].text
            price = row_columns[5].text
            
            _row = {
                'Дата IPO': date_ipo,
                'Компания': company_name,
                'Биржа': exchange_name,
                'Оценка IPO': estimation_ipo,
                'Цена IPO': price_ipo,
                'Цена': price
            }

            res_list.append(_row)
        
        return self.validate_data(pd.json_normalize(res_list))

    async def parse_data(self) -> pd.DataFrame:
        
        logger.info('START: Parsing data from Investings')

        today = pendulum.now('Europe/Moscow').date()
        prev_year = today.subtract(years=1).year
        start = pendulum.date(year=prev_year, month=1, day=1).format('Y-MM-DD')
        end = today.format('Y-MM-DD')

        df_old = await self._parse_part_of_data(start=start, end=end)
        start = df_old['Дата IPO'].max()

        if len(df_old) == 200:
            df_append = df_old
            while len(df_append) == 200:
                df_append = await self._parse_part_of_data(start=start, end=end)
                start = df_append['Дата IPO'].max()
                df_appended = pd.concat([df_old, df_append], axis=0)
                df_appended.drop_duplicates(inplace=True)

            logger.info('END: Parsing data from Investings')
            return df_appended, 'China'
        else: 
            logger.info('END: Parsing data from Investings')
            return df_old, 'China'
            
async def main():
    parser = InvestingsParser()
    df, _ = await parser.parse_data()
    df.to_csv('output.csv')

if __name__ == '__main__':
    asyncio.run(main())