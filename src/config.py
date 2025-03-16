import os
from loguru import logger
from dotenv import load_dotenv

from .utils import FileValidator, FileHandler

class Config:
    def __init__(self) -> None:
        # === Path Configurations ===
        self.BASE_DIR = os.getcwd()
        self.PATH_TO_VALIDATE = {
            "plot_settings": os.path.join(
                self.BASE_DIR, "./settings/plot_settings", "settings.yaml"
            ),
            "telegram_tokens": os.path.join(
                self.BASE_DIR, "./settings/telegram_api", "tokens.env"
            ),
            "telegram_info": os.path.join(
                self.BASE_DIR, "./settings/telegram_api", "telegram_info.yaml"
            ),
            # "where_push": os.path.join(
            #     self.BASE_DIR, "./settings/telegram_api/where_push.yaml"
            # ),
            # "icon_logo": os.path.join(
            #     self.BASE_DIR, "./items/icons/", "headlins_logotip.png"
            # ),
            # "icon_watermark": os.path.join(
            #     self.BASE_DIR, "./items/icons/", "telegram+name_canals_osnova.png"
            # ),
            "font_bold": os.path.join(
                self.BASE_DIR, "./items/my_font/Inter/", "Inter-Bold.otf"
            ),
            "font_regular": os.path.join(
                self.BASE_DIR, "./items/my_font/Inter/", "Inter-Regular.otf"
            ),
            "data_EU_month": os.path.join(
                self.BASE_DIR, "./items/data/", "Euronext_by_month.csv"
            ),
            "data_EU_year": os.path.join(
                self.BASE_DIR, "./items/data/", "Euronext_by_year.csv"
            ),
            "data_CN_month": os.path.join(
                self.BASE_DIR, "./items/data/", "Investings_by_month.csv"
            ),
            "data_CN_year": os.path.join(
                self.BASE_DIR, "./items/data/", "Investings_by_year.csv"
            ),
            "data_RU_month": os.path.join(
                self.BASE_DIR, "./items/data/", "Preqveca_by_month.csv"
            ),
            "data_RU_year": os.path.join(
                self.BASE_DIR, "./items/data/", "Preqveca_by_year.csv"
            ),
            "data_US_month": os.path.join(
                self.BASE_DIR, "./items/data/", "Stockanalysis_by_month.csv"
            ),
            "data_US_year": os.path.join(
                self.BASE_DIR, "./items/data/", "Stockanalysis_by_year.csv"
            ),
        }

        # === Path Validation ===
        logger.info("Start checking for validity of paths to configuration files.")
        for _, path in self.PATH_TO_VALIDATE.items():
            FileValidator.validate_file_path(path)
        logger.info("All file paths have been validated successfully.")

        # === Telegram Configuration ===
        load_dotenv(self.PATH_TO_VALIDATE["telegram_tokens"])
        self.TELEGRAM_API_TOKEN = os.getenv('TEST_BOT_TOKEN')
        self.telegram_info = FileHandler.load_yaml(
            self.PATH_TO_VALIDATE["telegram_info"]
        )

        # === Settings Loading ===
        self.SETTINGS_PLOTS = FileHandler.load_yaml(
            self.PATH_TO_VALIDATE['plot_settings']
        )

    def get_telegram_api_token(self) -> str:
        return self.TELEGRAM_API_TOKEN

    def get_telegram_chat_id(self) -> str:
        return self.telegram_info['TEST_GROUP_ID']

    def get_plot_settings(self) -> str:
        return self.SETTINGS_PLOTS

    def get_paths(self) -> str:
        return self.PATH_TO_VALIDATE