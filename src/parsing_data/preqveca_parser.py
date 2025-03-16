from httpx import AsyncClient, QueryParams, Response 
import asyncio
import pandas as pd
import pendulum
from bs4 import BeautifulSoup
from loguru import logger

class PreqvecaParser:
    def __init__(self) -> None:
        self._http_client = AsyncClient()

    async def _request(self, url: str, params: QueryParams | None = None) -> Response:
        response = await self._http_client.get(url, params=params)
        response.raise_for_status()
        return response
        
    async def parse_data(self, ipo_mode: int = 1) -> pd.DataFrame:

        logger.info('START: Parsing data from Preqveca')

        today = pendulum.now('Europe/Moscow').date()
        prev_year = today.subtract(years=5).year
        start = pendulum.date(year=prev_year, month=1, day=1).format('DD.MM.Y')
        end = today.format('DD.MM.Y')

        url = 'https://preqveca.ru/placements/'

        aux_list: list[dict] = list()

        count: int = 0

        params_types = {
            'sf[ipo_t]': str(),
            'sf[ipo]': str(0),
            'sf[status]': str(0),
            'sf[countr]': str(2),
            'sf[spec]': str(0),
            'sf[listing]': str(0),
            'sf[psf]': str(start),
            'sf[pst]': str(end),
            'sf[pt]': str(ipo_mode),
            'sf[ind]': str(0),
            'sf[pef]': str(),
            'sf[pet]': str(),
            'rec_start': str(count)
        }

        params = QueryParams(params_types)

        while True:
            
            # params['rec_start'] = count

            params = params.set('rec_start', str(count))

            try:
                response = await self._request(url=url, params=params)
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', class_='datagrid')
                if table is not None:
                    rows = table.find_all('tr')
                else:
                    break
            except Exception as e:
                logger.error(f'Error while parsing Preqveca')
                break
                      
            for num, row in enumerate(rows):
                if num == 0:
                    headers = row.find_all('th')
                    headers_list = [header.text for header in headers]
                else:
                    _dict = dict()
                    for j, data in enumerate(row.find_all('td')):
                        _dict[headers_list[j]] = data.text
                    aux_list.append(_dict)

            count += 30

        logger.info('END: Parsing data from Preqveca')
        
        return pd.json_normalize(aux_list), 'Russia'

async def main():
    parser = PreqvecaParser()
    df, _ = await parser.parse_data()
    df.to_csv('output.csv')

if __name__ == '__main__':
    asyncio.run(main())