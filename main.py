from src import DataValidator
from src import EuronextParser
from src import StockanalysisParser
from src import PreqvecaParser
from src import InvestingsParser
from src import DataCalculator
from src import DataUpdater
from src import PlotCreator
from src import PlotSender
from src import Config
import pandas as pd
import asyncio
import pendulum
from loguru import logger

async def main():

    start_time = pendulum.now('Europe/Moscow').time()

    # ============ Init config ============
    config = Config()
    plot_settings = config.get_plot_settings()
    paths = config.get_paths()
    group_id = config.get_telegram_chat_id()
    token = config.get_telegram_api_token()

    # ============ Init submodules ============
    validator = DataValidator()
    euro_parser = EuronextParser()
    stock_parser = StockanalysisParser()
    preq_parser = PreqvecaParser()
    invest_parser = InvestingsParser()
    calculator = DataCalculator()
    updater = DataUpdater()
    plot_creator = PlotCreator(plot_settings=plot_settings, paths=paths)
    plot_sender = PlotSender(token=token, group_id=group_id)

    # ============ Load data ============

    logger.info('START: Load data.')

    df_us_prev_month = pd.read_csv(paths['data_US_month'], sep=';')
    df_us_prev_year = pd.read_csv(paths['data_US_year'], sep=';')

    df_rus_prev_month = pd.read_csv(paths['data_RU_month'], sep=';')
    df_rus_prev_year = pd.read_csv(paths['data_RU_year'], sep=';')

    df_euro_prev_month = pd.read_csv(paths['data_EU_month'], sep=';')
    df_euro_prev_year = pd.read_csv(paths['data_EU_year'], sep=';')

    df_china_prev_month = pd.read_csv(paths['data_CN_month'], sep=';')
    df_china_prev_year = pd.read_csv(paths['data_CN_year'], sep=';')

    logger.info('END: Load data.')

    # ============ Parsing data ============

    logger.info('START: Parsing data.')

    df_euro, euro_type = await euro_parser.parse_data()
    df_stock, us_type = await stock_parser.parse_data()
    df_preq, rus_type = await preq_parser.parse_data()
    df_invest, china_type = await invest_parser.parse_data()

    logger.info('END: Parsing data.')
    
    # ============ Validate data ============

    logger.info('START: Validate data.')

    df_euro = validator.euronext_validator(df_euro)
    df_stock = validator.stockanalysis_validator(df_stock)
    df_preq = validator.preqveca_validator(df_preq)
    df_invest = validator.investings_validator(df_invest)

    logger.info('END: Validate data.')

    # ============ Create dataframes ============
    
    logger.info('START: Processing  data.')

    # ===== Monthly =====
    df_us_updated_month = updater.update_month_data(df_us_prev_month, df_stock, paths['data_US_month'], us_type)
    df_us_month = calculator.prepare_month_df(df_us_updated_month, us_type)     

    df_rus_updated_month = updater.update_month_data(df_rus_prev_month, df_preq, paths['data_RU_month'], rus_type)
    df_rus_month = calculator.prepare_month_df(df_rus_updated_month, rus_type)     

    df_euro_updated_month = updater.update_month_data(df_euro_prev_month, df_euro, paths['data_EU_month'], euro_type)
    df_euro_month = calculator.prepare_month_df(df_euro_updated_month, euro_type) 

    df_china_updated_month = updater.update_month_data(df_china_prev_month, df_invest, paths['data_CN_month'], china_type)
    df_china_month = calculator.prepare_month_df(df_china_updated_month, china_type) 

    # ===== Yearly =====
    df_us_year = calculator.prepare_year_df(df_stock, us_type)
    updater.update_year_data(df_us_prev_year, df_us_year, paths['data_US_year'], us_type)

    df_rus_year = calculator.prepare_year_df(df_preq, rus_type)
    updater.update_year_data(df_rus_prev_year, df_rus_year, paths['data_RU_year'], rus_type)

    df_euro_year = calculator.prepare_year_df(df_euro, euro_type)
    updater.update_year_data(df_euro_prev_year, df_euro_year, paths['data_EU_year'], euro_type)

    df_china_year = calculator.prepare_year_df(df_invest, china_type)
    updater.update_year_data(df_china_prev_year, df_china_year, paths['data_CN_year'], china_type)

    logger.info('END: Processing  data.')

    # ============ Create plots ============

    logger.info('START: Create plots.')

    # ===== Monthly =====
    plot_us_month = plot_creator.generate_month_plot(df_us_month, 'США', caption_type=us_type)
    plot_rus_month = plot_creator.generate_month_plot(df_rus_month, 'Россия', caption_type=rus_type)
    plot_euro_month = plot_creator.generate_month_plot(df_euro_month, 'Европа', caption_type=euro_type)
    plot_china_month = plot_creator.generate_month_plot(df_china_month, 'Китай', caption_type=china_type)

    # ===== Yearly =====
    plot_us_year = plot_creator.generate_year_plot(df_us_prev_year, 'США', caption_type=us_type)
    plot_rus_year = plot_creator.generate_year_plot(df_rus_prev_year, 'Россия', caption_type=rus_type)
    plot_euro_year = plot_creator.generate_year_plot(df_euro_prev_year, 'Европа', caption_type=euro_type)
    plot_china_year = plot_creator.generate_year_plot(df_china_prev_year, 'Китай', caption_type=china_type)

    logger.info('END: Create plots.')
  
    # ============ Send plots ============

    logger.info('START: Sending plots.')

    # ===== Monthly =====
    await plot_sender.send_gragh(buf=plot_us_month, caption_type=us_type, yearly_type=False)
    await plot_sender.send_gragh(buf=plot_rus_month, caption_type=rus_type, yearly_type=False)
    await plot_sender.send_gragh(buf=plot_euro_month, caption_type=euro_type, yearly_type=False)
    await plot_sender.send_gragh(buf=plot_china_month, caption_type=china_type, yearly_type=False)

    # ===== Yearly =====
    await plot_sender.send_gragh(buf=plot_us_year, caption_type=us_type, yearly_type=True)
    await plot_sender.send_gragh(buf=plot_rus_year, caption_type=rus_type, yearly_type=True)
    await plot_sender.send_gragh(buf=plot_euro_year, caption_type=euro_type, yearly_type=True)
    await plot_sender.send_gragh(buf=plot_china_year, caption_type=china_type, yearly_type=True)

    logger.info('END: Sending plots.')

    end_time = pendulum.now('Europe/Moscow').time()

    logger.info(f'Complited successfully. Time of program execution is {end_time - start_time}.')

if __name__ == '__main__':
    asyncio.run(main())