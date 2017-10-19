# Copyright 2017 Tao Chen.  All rights reserved
# Copyright 2017 The D2VLab Developers



from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta, date
from urllib.request import Request, urlopen, urlretrieve
from urllib.error import URLError, HTTPError
import re
import sys
import pytz
import os
import time
import logging
import logging.handlers
import argparse


def get_html(url):
    """
    Get raw html from url
    """
    logger.info('downloading from ' + url)
    req = Request(url)
    try:
        response = urlopen(req)
        page = response.read()
        response.close()
        return page
    except HTTPError as e:
        logger.error("The server couldn\'t fulfill the request.", exc_info=True)
    except URLError as e:
        logger.error("We failed to reach a server.", exc_info=True)


def save_html(url, save_to_dir, file_name):
    """
    Save the html to the given location with given name
    """
    if not os.path.isdir(save_to_dir):
        print("{} directory does not exist".format(save_to_dir))
        return None

    file_path = save_to_dir + "/" + file_name
    try:
        urlretrieve(url, file_path)
        return file_path
    except IOError as e:
        logger.error("Failed writing to the file " + file_path, exc_info=True)
    except HTTPError as e:
        logger.error("The server couldn\'t fulfill the request.", exc_info=True)
    except URLError as e:
        logger.error("We failed to reach a server.", exc_info=True)

    logger.error("Failed saving html for url " + url)
    return None


def get_next_friday(current_date, distance=0):
    """
    Return the next Friday date object, given the current date object. distance determines how many weeks away,
    default is o meaning the immediate next Friday.
    """
    temp_d = current_date
    temp_d_weekday = temp_d.weekday()
    if temp_d_weekday > 4:
        temp_d += timedelta(5 + 6 - temp_d_weekday)
    else:
        temp_d += timedelta(4 - temp_d_weekday)
    temp_d += timedelta(distance * 7)
    return temp_d


def get_option_date_list(symbol, count=3):
    """
    Get a list of dates and epoch timestamps for the option page for a given symbol. YF uses linux timestamp for
    date.
    """
    current_date_in_utc = datetime.utcnow().replace(tzinfo=pytz.utc, hour=0, minute=0, second=0,
                                                    microsecond=0)
    distance = count
    list_of_option_date = []
    for d in range(distance):
        next_friday_date = get_next_friday(current_date_in_utc, d)
        next_friday_epoch_str = str(int(get_next_friday(current_date_in_utc, d).timestamp()))
        list_of_option_date.append({"date_str" : next_friday_date.strftime("%Y-%m-%d :q:Q:q%Z"),
                                    "epoch_str" : next_friday_epoch_str})
    return list_of_option_date


def parse_option_page(html, expiration_date_str, est_time, is_call=True):
    # load file
    if not os.path.exists(html):
        logger.error("{} html file does not exist".format(html))
        return None

    with open(html, 'r') as file:
        try:
            # second parse the html
            soup = BeautifulSoup(file, 'lxml')
            current_stock_price = soup.find("span", {"data-reactid": "35"}).text
            yf_search_dict = {"ContractName": "data-col0", "LastTradeDate": "data-col1", "Strike": "data-col2",
                              "LastPrice": "data-col3", "Bid": "data-col4", "Ask": "data-col5", "Change": "data-col6",
                              "PercentChange": "data-col7", "Volume": "data-col8", "OpenInterest": "data-col9",
                              "ImpliedVolatility": "data-col10"}
            #  extract
            options = soup.find("table", {"class": "calls"})
            option_type = "Call"
            if not is_call:
                options = soup.find("table", {"class": "puts"})
                option_type = "Put"

            if options is None:
                logger.error("Did not find options section in saved html file {}".format(html))
                return None

            row_cnt = 0
            row = options.find("tr", {"class": "data-row" + str(row_cnt)})
            option_rows = []
            while row is not None:
                result_list = []
                # TimeStamp, CurrentStockPrice, ContractType, ContractExpiration, ContractName, LastTradeDate, Strike,
                # LastPrice, Bid, Ask, Change, PercentChange, Volume, OpenInterest, ImpliedVolatility
                result_list.append(est_time)
                result_list.append(current_stock_price)
                result_list.append(option_type)
                result_list.append(expiration_date_str)
                result_list.append(row.find("td", {"class": yf_search_dict.get("ContractName")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("LastTradeDate")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("Strike")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("LastPrice")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("Bid")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("Ask")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("Change")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("PercentChange")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("Volume")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("OpenInterest")}).text)
                result_list.append(row.find("td", {"class": yf_search_dict.get("ImpliedVolatility")}).text)

                option_rows.append('|'.join(result_list))
                row_cnt += 1
                row = options.find("tr", {"class": "data-row" + str(row_cnt)})
            return option_rows
        except Exception as e:
            logger.error("Failed reading from the html file " + html, exc_info=True)


def now_in_eastern_time():
    est_tz = pytz.timezone('US/Eastern')
    timestamp_format_string = '%Y-%m-%d %H:%M %Z'
    est_time = datetime.now(est_tz).strftime(timestamp_format_string)  # we are getting EST time
    return est_time


def get_option_page_url(symbol, option_date):
    """
    Get option page url from symbol and option_date string
    """
    url_base = "https://finance.yahoo.com/quote/" + symbol + "/options?p=" + symbol + "&date="
    return url_base + option_date


def get_archive_daily_option_page_file(symbol, option_date):
    """
    Get archive daily option page file name
    """
    est_tz = pytz.timezone('US/Eastern')
    est_time = datetime.now(est_tz).strftime("%Y-%m-%d-%H-%M-%Z")  # we are getting EST time
    return "yf-{}-option-html-{}-{}".format(symbol, option_date, est_time)


def get_daily_option_report_file(symbol, option_date):
    """
    Get daily option report file
    """
    est_tz = pytz.timezone('US/Eastern')
    est_date = datetime.now(est_tz).strftime("%Y-%m-%d-%Z")  # we are getting EST date
    return "yf-{}-option-report-{}-{}".format(symbol, option_date, est_date)


def save_daily_option_report(row_list, save_to_dir, file_name):
    """
    Save the given row_list to given dir with given file name
    """
    if not os.path.isdir(save_to_dir):
        print("{} directory does not exist".format(save_to_dir))
        return None

    file_path = save_to_dir + "/" + file_name

    try:
        with open(file_path, "a") as my_file:
            for row in row_list:
                my_file.write(row + '\n')
        return file_path
    except IOError as e:
        logger.error("Failed writing to the file " + file_path, exc_info=True)


def main(symbol):
    archive_dir = "archive"
    report_dir = "reports"
    options_expiration_counts = 8

    list_of_option_date = get_option_date_list(symbol, options_expiration_counts)
    # first save
    option_pages_to_parse = []
    start = time.time()
    failed_count = 0
    logger.info("Start saving raw htmls for {} to {} dir ...".format(symbol, archive_dir))
    # print("Start saving raw htmls for {} to {} dir ...".format(symbol, archive_dir))
    for option_date in list_of_option_date:
        option_date_epoch = option_date["epoch_str"]
        archived_file = save_html(get_option_page_url(symbol, option_date_epoch), archive_dir,
                                  get_archive_daily_option_page_file(symbol, option_date_epoch))
        if archived_file is None:
            logger.warning("Failed saving raw html for url {} ".format(get_option_page_url(symbol, option_date_epoch)))
            failed_count += 1
        else:
            timestamp = now_in_eastern_time()
            option_pages_to_parse.append({"archive_file": archived_file, "option_date": option_date, "est_time":
                                          timestamp})
    end = time.time()
    logger.info("Successfully saved {} raw html files and failed saving {} raw html files in {} seconds.".format(len(
        option_pages_to_parse), failed_count, end - start))
    # second generate report
    start = time.time()
    logger.info("Start generating reports for {} in {} dir ...".format(symbol, report_dir))
    success_cnt = 0
    for option_page_info in option_pages_to_parse:
        # first calls
        option_date_str = option_page_info["option_date"]["date_str"]
        option_date_epoch = option_page_info["option_date"]["epoch_str"]
        call_rows = parse_option_page(option_page_info["archive_file"], option_date_str, option_page_info["est_time"],
                                      True)
        put_rows = parse_option_page(option_page_info["archive_file"], option_date_str, option_page_info["est_time"],
                                     False)

        if (call_rows is None) or (put_rows is None):
            logger.error("Failed generating report for {}. Did not find any call/put rows. ".format(
                option_page_info["archive_file"]))
        else:
            all_rows = []
            all_rows.extend(call_rows)
            all_rows.extend(put_rows)
            report = save_daily_option_report(all_rows, report_dir,
                                              get_daily_option_report_file(symbol, option_date_epoch))
            if report is None:
                logger.error(
                    "Failed generating report for {}. Save is failed. ".format(option_page_info["archive_file"]))
            else:
                logger.info("Successfully generated report {} with {} rows.".format(report, len(all_rows)))
                success_cnt += 1
    end = time.time()
    logger.info("Successfully generated {} report files in {} seconds.".format(success_cnt, end - start))


def configure_logger(symbol, logger):
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.handlers.TimedRotatingFileHandler("logs/" + symbol + "-crawler.log", when='D', interval=1)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)


parser = argparse.ArgumentParser()
parser.add_argument("symbol", help="the stock/option symbol you want to crawl data for")
args = parser.parse_args()
symbol = args.symbol.upper()

logger = logging.getLogger("yfcrawler")
configure_logger(symbol, logger)
if __name__ == "__main__":
    main(symbol)
