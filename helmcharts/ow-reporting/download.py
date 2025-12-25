import os
import requests
from datetime import datetime
import pytz
from tqdm import tqdm

PAR_DOWNLOAD_URL_TEMPLATE = ""
KSA_TIMEZONE = pytz.timezone("Asia/Riyadh")

import logging
logging.basicConfig(level=logging.INFO, 
                   format="%(asctime)s - %(levelname)s - %(message)s")


def generate_monthly_filename():
    current_time = datetime.now(KSA_TIMEZONE)
    return f"report_{current_time.strftime('%Y_%m')}.csv"


def fetch_file(url, save_path):
    logging.info(f"Starting download from: {url}")
    logging.info(f"Target location: {save_path}")

    try:
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            logging.error(f"Download unsuccessful. Status: {response.status_code}")
            return

        file_size = int(response.headers.get("content-length", 0))

        with open(save_path, "wb") as output_file, tqdm(
            desc=save_path,
            total=file_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress:
            for data in response.iter_content(chunk_size=1024):
                if data:
                    output_file.write(data)
                    progress.update(len(data))

        logging.info("Download completed successfully.")
    except Exception as error:
        logging.error(f"Download error: {error}")


def execute_download():
    filename = generate_monthly_filename()
    output_path = f"{filename}"

    download_url = PAR_DOWNLOAD_URL_TEMPLATE.format(filename=filename)

    fetch_file(download_url, output_path)


if __name__ == "__main__":
    execute_download()
