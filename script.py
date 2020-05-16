## Sample script to show the import

## Usage:
## python script.py --help
## python script.py -sender hmn24NASDAQScrape@gmail.com -receiver hmn24NASDAQScrape@gmail.com 

import utils.extract as extract
import utils.email as email
import argparse

parser = argparse.ArgumentParser(
    description="To generate filtered ticks and email"
)

## Designate required args
parser.add_argument("-sender", required=True, help="Sender Email")
parser.add_argument("-receiver", required=True, help="Receiver Email")

## Parse script argument
args = parser.parse_args()

## Generate all the relevant filtered ticks for the past 250 days
extract.getAndStoreFilteredTicks()

## Send an email to designated user
email.sendTickEmails(args.sender, args.receiver, 'Report containing Overbought/Oversold Tickers', extract.readFilteredTicks())