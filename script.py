
# pylint: disable=invalid-name

"""
Sample script to show the import

Usage:
python script.py --help
python script.py -sender hmn24NASDAQScrape@gmail.com -receiver hmn24NASDAQScrape@gmail.com
"""

import argparse

import libs.NASDAQextract as NASDAQext
import libs.email as eml

parser = argparse.ArgumentParser(description="To generate filtered ticks and email")

## Designate required args
parser.add_argument("-sender", required=True, help="Sender Email")
parser.add_argument("-receiver", required=True, help="Receiver Email")

## Parse script argument
args = parser.parse_args()

## Generate all the relevant NASDAQ filtered ticks for the past 250 days
NASDAQext.getAndStoreFilteredTicks()

## Send an email to designated user
eml.sendTickEmails(
    args.sender,
    args.receiver,
    "Report containing Overbought/Oversold Tickers",
    NASDAQext.readFilteredTicks(),
)
