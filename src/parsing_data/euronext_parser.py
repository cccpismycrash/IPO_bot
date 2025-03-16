from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import pendulum
from httpx import AsyncClient, QueryParams, Response
from loguru import logger

class EuronextParser:
    def __init__(self) -> None:
        self._http_client = AsyncClient()

    async def _request(self, url: str, params: QueryParams | None = None) -> Response:
        response = await self._http_client.get(url, params=params)
        response.raise_for_status()
        return response
    
    async def parse_data(self) -> pd.DataFrame:

        logger.info('START: Parsing data from Euronext')

        page = 0
        aux_dict: dict[str, list] = dict()

        today = pendulum.now('Europe/Moscow').date()
        prev_year = today.subtract(years=1).year
        start = pendulum.date(year=prev_year, month=1, day=1).format('MM/DD/Y')
        end = today.format('MM/DD/Y')

        params_types = {
            'combine': str(),
            'field_iponi_ipo_date_value[min]': str(start),
            'field_iponi_ipo_date_value[max]': str(end),
            'page': str(page)
        }

        params = QueryParams(params_types)

        while True:

            url = 'https://live.euronext.com/en/ipo-showcase'

            params = params.set('page', str(page))
            
            try:
                response = await self._request(url, params=params)
            except Exception as e:
                logger.error(f'Error while parsing Euronext')
                break

            soup = BeautifulSoup(response.text, 'html.parser')

            links = soup.find_all('tr')

            if not links:
                break

            for num, link in enumerate(links):
                if num == 0:
                    columns_list = []
                    items = link.find_all('th')
                    for item in items:
                        item = item.text
                        columns_list.append(item)
                        if aux_dict.get(item) is None:
                            aux_dict[item] = []
                        else:
                            pass
                else:
                    items = link.find_all('td')
        
                    for j, item in enumerate(items):
                        value = item.text.strip()
                        aux_dict[columns_list[j]].append(value)

            page += 1

        logger.info('END: Parsing data from Euronext')

        return pd.DataFrame.from_dict(aux_dict), 'Europe'
    
async def main():
    parser = EuronextParser()
    df, _ = await parser.parse_data()
    df.to_csv('output.csv')

if __name__ == '__main__':
    asyncio.run(main())