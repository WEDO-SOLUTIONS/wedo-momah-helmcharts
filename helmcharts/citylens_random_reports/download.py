import os
import requests
from datetime import datetime
import pytz
from tqdm import tqdm

# --- Configuration ---
PAR_DOWNLOAD_URL_TEMPLATE = "https://axownvq9lhmx.objectstorage.me-jeddah-1.oci.customer-oci.com/p/RLRHXO2sNpitqqvOqSuF_6bYV_h7ejPBhJJ2qq1oNhHmIE94Yg0bD0edV-qLnL_J/n/axownvq9lhmx/b/prod-citylens-reports/o/{filename}"
KSA_TZ = pytz.timezone("Asia/Riyadh")

# Logging setup
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_current_month_file_name():
    now = datetime.now(KSA_TZ)
    return f"report_{now.strftime('%Y_%m')}.csv"


def download_file(download_url, output_file):
    """
    Downloads a file from the given URL and saves it locally.
    """
    logging.info(f"Downloading file from: {download_url}")
    logging.info(f"Saving file to: {output_file}")

    try:
        # Send a GET request to the PAR URL
        response = requests.get(download_url, stream=True)
        if response.status_code != 200:
            logging.error(f"Failed to download file. Status code: {response.status_code}")
            return

        # Get the total file size from the response headers
        total_size = int(response.headers.get("content-length", 0))

        # Use tqdm to display a progress bar
        with open(output_file, "wb") as file, tqdm(
            desc=output_file,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    progress_bar.update(len(chunk))

        logging.info("✅ File downloaded successfully.")
    except Exception as e:
        logging.error(f"❌ Download failed: {e}")


def main():
    # Generate the current month's file name
    file_name = get_current_month_file_name()
    output_file = f"downloaded_{file_name}"

    # Construct the full download URL
    download_url = PAR_DOWNLOAD_URL_TEMPLATE.format(filename=file_name)

    # Download the file
    download_file(download_url, output_file)


if __name__ == "__main__":
    main()
