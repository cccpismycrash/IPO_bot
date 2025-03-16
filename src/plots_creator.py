import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import pandas as pd
import pendulum
from io import BytesIO
from matplotlib.axes import Axes
from matplotlib.figure import Figure
import math
from matplotlib.ticker import MaxNLocator
from loguru import logger

class PlotCreator:

    def __init__(self, 
                 plot_settings: dict, 
                 paths: dict
                 ) -> None:
        
        self.plot_settings = plot_settings
        self.paths = paths

    def _sort_month(self, 
                    df: pd.DataFrame
                    ) -> pd.DataFrame:
        
        month_order = ['January',
                       'February',
                       'March',
                       'April',
                       'May',
                       'June',
                       'July',
                       'August',
                       'September',
                       'October',
                       'November',
                       'December']
        
        df['MonthIndex'] = df['Month'].apply(lambda x: month_order.index(x))
        df = df.sort_values('MonthIndex').drop('MonthIndex', axis=1)
        return df

    def _replace_month(self, 
                       date_string: str
                       ) -> str:

        month_dict = {
            'January': 'январь',
            'February': 'февраль',
            'March': 'март',
            'April': 'апрель',
            'May': 'май',
            'June': 'июнь',
            'July': 'июль',
            'August': 'август',
            'September': 'сентябрь',
            'October' : 'октябрь',
            'November': 'ноябрь',
            'December' : 'декабрь'
        }

        for month_name, ru_month_name in month_dict.items():
            if month_name in date_string:
                date_string = date_string.replace(month_name, ru_month_name)
                break

        return date_string

    def _make_month_figure(self, 
                           df: pd.DataFrame
                           ) -> tuple[Figure, Axes]:    
        
        fig, ax = plt.subplots(figsize=(15, 9))
        ax.bar(x=df['Month'], height=df['Quantity'], color=self.plot_settings['color_plot'])

        return fig, ax

    def _make_year_figure(self, df: pd.DataFrame) -> tuple[Figure, Axes]:    
        
        fig, ax = plt.subplots(figsize=(15, 9))
        ax.bar(x=df['Year'], height=df['Quantity'], color=self.plot_settings['color_plot'])

        return fig, ax

    def _max_lim_axes(self,
                      df: pd.DataFrame
                      ) -> float:

        _max = df['Quantity'].max()
        _len = len(str(_max))
        if _len == 1:
            max_lim = 10 * 1.01
        elif _len == 2 or _len == 3:
            max_lim = math.ceil((_max) / 10**(_len - 1)) * 10**(_len - 1) * 1.01
        elif _len >= 4:
            max_lim = math.ceil((_max) / 10**(_len - 2)) * 10**(_len - 2) * 1.01

        return max_lim

    def _customize_month_plot(self, 
                              ax: Axes, 
                              df: pd.DataFrame
                              ) -> None:
        
        max_lim = self._max_lim_axes(df)

        level = max_lim * 0.01

        ax.set_ylim(bottom=0, top=max_lim)
        
        labels_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['value_labelsize_monthly'])
        for i, value in enumerate(df['Quantity']):
            ax.text(i, value + level, str(value), ha='center', fontsize=self.plot_settings['text_fontsize'], fontproperties=labels_font_bold)

        ax.spines['top'].set_position(('outward', 15))
        ax.spines['top'].set_linewidth(2)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)

        inter_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['labelsize_x_ticks_monthly'])
        for label in ax.get_xticklabels():
            label.set_fontproperties(inter_font_bold)
            label.set_color(self.plot_settings['color_sign_axis'])

        inter_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['labelsize_y_ticks_monthly'])
        for label in ax.get_yticklabels():
            label.set_fontproperties(inter_font_bold)
            label.set_color(self.plot_settings['color_sign_axis'])

    def _customize_year_plot(self, 
                             ax: Axes, 
                             df: pd.DataFrame
                             ) -> None:

        max_lim = self._max_lim_axes(df)

        level = max_lim * 0.01

        ax.set_ylim(bottom=0, top=max_lim)

        labels_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['value_labelsize_yearly'])
        for i, value in zip(df['Year'], df['Quantity']):
            ax.text(i, value + level, str(value), ha='center', fontsize=self.plot_settings['text_fontsize'], fontproperties=labels_font_bold)

        ax.spines['top'].set_position(('outward', 15))
        ax.spines['top'].set_linewidth(2)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)

        ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=7))

        inter_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['labelsize_x_ticks_yearly'])
        for label in ax.get_xticklabels():
            label.set_fontproperties(inter_font_bold)
            label.set_color(self.plot_settings['color_sign_axis'])

        inter_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['labelsize_y_ticks_yearly'])
        for label in ax.get_yticklabels():
            label.set_fontproperties(inter_font_bold)
            label.set_color(self.plot_settings['color_sign_axis'])


    def _add_month_title(self, 
                         ax: Axes, 
                         df: pd.DataFrame, 
                         region: str
                         ) -> None:

        count = int(df.loc[df['Month'] == pendulum.now('Europe/Moscow').format('MMMM'), 'Quantity'])
        month = self._replace_month(pendulum.now('Europe/Moscow').format('MMMM'))

        inter_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['title_fontsize_monthly'])
        ax.set_title(f'Количество IPO за {month}: {count} ({region})', loc='left', fontproperties=inter_font_bold, y=self.plot_settings['header_offset_monthly'])

    def _add_year_title(self, ax: Axes, region: str, fig: Figure) -> None:

        inter_font_bold = FontProperties(fname=self.paths['font_bold'], weight='bold', size=self.plot_settings['title_fontsize_yearly'])
        ax.set_title(f'Количество проведенных IPO за год ({region})', loc='left', fontproperties=inter_font_bold, y=self.plot_settings['header_offset_yearly'])

    def generate_month_plot(self, 
                            df: pd.DataFrame, 
                            region: str,
                            caption_type: str
                            ) -> BytesIO:

        logger.info(f'START: Generate monthly plot for IPO in {caption_type} region.')

        df = self._sort_month(df)
        fig, ax = self._make_month_figure(df)
        self._customize_month_plot(ax, df)
        self._add_month_title(ax, df, region)
        
        buf = BytesIO()
        plt.savefig(buf, format='jpg', bbox_inches='tight')
        buf.seek(0)

        logger.info(f'END: Generate monthly plot for IPO in {caption_type} region.')

        return buf

    def generate_year_plot(self, 
                           df: pd.DataFrame, 
                           region: str,
                           caption_type: str
                           ) -> BytesIO:

        logger.info(f'START: Generate yearly plot for IPO in {caption_type} region.')
        
        fig, ax = self._make_year_figure(df)
        self._customize_year_plot(ax, df)
        self._add_year_title(ax, region, fig)
        
        buf = BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)

        logger.info(f'END: Generate yearly plot for IPO in {caption_type} region.')

        return buf