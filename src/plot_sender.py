from aiogram import Bot, types
from tenacity import retry, stop_after_delay, wait_fixed
from io import BytesIO
from loguru import logger
from aiogram.types import InputMediaPhoto, BufferedInputFile

class PlotSender:

    def __init__(self, 
                 token: str, 
                 group_id: str
                 ) -> None:
        
        self.token = token
        self.bot = Bot(self.token)
        self.group_id = group_id

    def _generate_yearly_caption(self, 
                                 caption_type: str
                                 ) -> str:
        
        caption_dict = {
            'Europe': (
                '#sentiment\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (Европа, биржа Euronext) за период с 2011-н.в.</i>\n' 
                '<i>** Статистика с 1995 по 2011 год <a href="https://site.warrington.ufl.edu/ritter/files/2015/04/Economies-of-scope-and-IPO-activity-in-Europe-2013.pdf">бралась</a> с учетом LSE.</i>\n\n'
                '<i><a href="https://live.euronext.com/en/ipo-showcase">Источник</a></i>'
            ),
            'China': (
                '#sentiment\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (Китай) за период с 2007 года.</i>\n\n'
                '<i><a href="https://www.investing.com/ipo-calendar/">Источник </a></i>'
                ),
            'Russia': (
                '#sentiment\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (Россия) за период с 2000 года.</i>\n\n'
                '<i><a href="https://preqveca.ru/placements/">Источник</a></i>'
            ),
            'US': (
                '#sentiment\n'
                '#US\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (США) за период с 1995 года.</i>\n\n'
                '<i><a href="https://stockanalysis.com/ipos/statistics/">Источник</a></i>'
            )
        }

        return caption_dict[caption_type]

    def _generate_monthly_caption(self, 
                                  caption_type: str
                                  ) -> str:
        
        caption_dict = {
            'Europe': (
                '#sentiment\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (Европа, биржа Euronext) за текущий год.</i>\n\n'
                '<i><a href="https://live.euronext.com/en/ipo-showcase">Источник</a></i>'
            ),
            'China': (
                '#sentiment\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (Китай) за текущий год.</i>\n\n'
                '<i><a href="https://www.investing.com/ipo-calendar/">Источник</a></i>'
            ),
            'Russia': (
                '#sentiment\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (Россия) за текущий год.</i>\n\n'
                '<i><a href="https://preqveca.ru/placements/">Источник</a></i>'
            ),
            'US': (
                '#sentiment\n'
                '#US\n'
                '#IPO\n\n'
                '<i>* График выходит на этом канале 29 числа каждого месяца в 18:00 по МСК и отражает статистику по IPO (США) за текущий год.\n\n</i>'
                '<i><a href="https://stockanalysis.com/ipos/statistics/">Источник</a></i>'
            )
        }

        return caption_dict[caption_type]

    @retry(stop=stop_after_delay(60 * 15), wait=wait_fixed(1))
    async def send_gragh(self,
                         buf: BytesIO,
                         caption_type: str,
                         yearly_type: bool
                         ) -> None:
        
        async with self.bot as bot:
            try:

                if yearly_type:
                    period = 'yearly'
                else:
                    period = 'monthly'

                logger.info(f'START: Sending {period} plot of IPO in {caption_type} region.')

                media = BufferedInputFile(file=buf.getvalue(), filename=f'{caption_type}_image.jpg')
                if yearly_type:
                    caption = self._generate_yearly_caption(caption_type=caption_type)
                else:
                    caption = self._generate_monthly_caption(caption_type=caption_type)
                await bot.send_media_group(chat_id=self.group_id,
                                           media=[InputMediaPhoto(type='photo', media=media, caption=caption, parse_mode='HTML')])
                
                logger.info(f'END: Sending {period} plot of IPO in {caption_type} region. Size of buffer: {round(len(buf.getvalue()), 1) / 1024} KB.')

            except Exception as e:
                logger.info(f'An error occurred while sending photo: {e}.')
                raise