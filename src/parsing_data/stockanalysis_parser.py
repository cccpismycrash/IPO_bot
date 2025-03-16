from httpx import AsyncClient, QueryParams, Response
import pandas as pd
from bs4 import BeautifulSoup
import pendulum
import asyncio
from loguru import logger

class StockanalysisParser:
    def __init__(self) -> None:
        self._http_client = AsyncClient()

    async def _request(self, url: str, params: QueryParams | None = None) -> Response:
        response = await self._http_client.get(url, params=params)
        response.raise_for_status()
        return response
        
    async def parse_data(self) -> pd.DataFrame:

        logger.info('START: Parsing data from Stockanalysis')

        aux_dict: dict[str, list] = dict()
        year = pendulum.now('Europe/Moscow').year
        while year != pendulum.now('Europe/Moscow').subtract(years=2).year:

            try:
                url = f'https://stockanalysis.com/ipos/{year}/'
                response = await self._request(url=url)
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', id='main-table')
                links = table.find_all('tr')
            except Exception as e:
                logger.error(f'Error while parsing Stockanalysis')
                break

            for num, link in enumerate(links):

                if num == 0:
                    columns_list = []
                    items = link.findAll('th')
                    for item in items:
                        clear_text = item.text.strip()
                        columns_list.append(clear_text)
                        if aux_dict.get(clear_text) is None:
                            aux_dict[clear_text] = []
                        else:
                            pass
                else:
                    items = link.findAll('td')
                    
                    for j, item in enumerate(items):
                        value = item.text.strip()
                        aux_dict[columns_list[j]].append(value)

            year -= 1

        logger.info('END: Parsing data from Stockanalysis')

        return pd.DataFrame.from_dict(aux_dict), 'US'
    
async def main():
    parser = StockanalysisParser()
    df, _ = await parser.parse_data()
    df.to_csv('output.csv')

if __name__ == '__main__':
    asyncio.run(main())