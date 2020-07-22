
# pylint: disable=invalid-name
# pylint: disable=missing-docstring

from selenium import webdriver

import plotly.express as px

import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd

indexMarkets = [
    "australia-200",
    "brazil-60",
    "cannabis-index",
    "china-a50",
    "china-h-shares",
    "Denmark25",
    "eu-stocks-50",
    "eu-volatility-index",
    "emerging-markets-index",
    "ftse-100",
    "ftse-250",
    "france-40",
    "germany-30",
    "germany-tech-30",
    "germany-mid-cap-50",
    "greece-20",
    "hong-kong-hs42",
    "hungary-12",
    "india-50",
    "italy-40",
    "japan-225",
    "malaysia-30",
    "holland-25",
    "norway-25",
    "portugal-20",
    "singapore-blue-chip",
    "south-africa-40",
    "sweden-30",
    "switzerland-blue-chip",
    "singapore-index",
    "spain-35",
    "taiwan-index-tw",
    "techmark",
    "us-spx-500",
    "us-tech-100",
    "USFANG",
    "russell-2000",
    "volatility-index",
    "wall-street",
]

IGprefix = "https://www.ig.com/sg/indices/markets-indices/"

def findXPath(driver, path):
    search = driver.find_elements_by_xpath(path)
    return search[0].text if search else None

def startBrowser():
    driver = webdriver.Firefox()
    driver.implicitly_wait(5)
    return driver

def genPairList(driver):

    pairList = []

    name, percent, direction = (
        "//h1[@class='ma__title']",
        "//span[@class='price-ticket__percent']",
        "//div[@class='information-popup']//strong",
    )

    for i in indexMarkets:
        temp = []
        try:
            ## Load webpage
            driver.get(IGprefix + i)

            ## Scrape required info
            for j in (name, percent, direction):
                res = findXPath(driver, j)
                if res:
                    temp.append(res)
                else:
                    break
            else:  ## nobreak -- Only append if (name,percent,direction) are all found
                pairList.append(temp)

        except Exception as e:
            print(f"Exception raised: {e}")
    
    return pd.DataFrame(pairList, columns=["Index", "Percent", "Direction"])

def genBarChart():

    global app

    fig = px.bar(
        df,
        x="Percent",
        y="Index",
        text="Index",
        color="Direction",
        color_discrete_map={"short": "blue", "long": "green"},
        height=1000,
        orientation="h"
    )
    fig.update_layout(
        title='IG Client Sentiment for Market Indices',
        yaxis_visible=False
    )

    # Generate app to run 
    app = dash.Dash()
    app.layout = html.Div([
        dcc.Graph(figure=fig)
    ])

    # Turn off reloader if inside Jupyter
    app.run_server(debug=True, use_reloader=False)

def scrapeAndPlot():

    ## Set as global variable
    global df

    wBrowser = startBrowser()
    df = genPairList(wBrowser)
    wBrowser.quit()

    ## Ctrl + C to exit plots once done
    ## Only plot if df is non-empty
    if len(df):
        genBarChart()

    print("Scraped and plotted successfully")
