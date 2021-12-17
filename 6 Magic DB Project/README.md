# Magic DB

## Data Engineering Capstone Project

### Project Summary

Magic: The Gathering (MTG) is a popular card game from the 90s that has a solid fan base and active trading card community. The game competitiveness and complexity attracts fans all around the world, creating a high demand for cards in the market.

The card trading market for MTG is complex given that number of players, the geographical spread of the game and the professional scenario. Even the construction of a new deck by the fan base can sky rocket the price of a single card within a day. In addition, official cards are released seasonally and many game stores buy and sell MTG products to casual or professional players making card prices volatile.

In the following sections we will explore the data and explain the steps taken.

### Scope
This project proposes the construction of a Magic database with card dimensional data and the prices that changes every day. This information is gathered and optimized for a fictional Data Science team to utilize in order to predict card prices in the next days. 

To accomplish this, we created an Airflow pipeline that extract the data from public hosts [Scryfall](https://scryfall.com) and [MTGJson](https://mtgjson.com), creates the source datasets for the project, load the dataset into staging tables in Redshift and finally populates the dimension and fact tables in a star schema also in Redshift. The schema is created to optimize card price queries.

### Describe and Gather Data
In this project, we will collect data from two distinct sources:

- [Scryfall](https://scryfall.com): following the [guidelines](https://scryfall.com/docs/api) of [MTG policy](https://company.wizards.com/en/legal/fancontentpolicy) this site provides an API to search MTG cards and detailed information, even the card images are available to request. In this project, we will request programmatically the [bulk data](https://scryfall.com/docs/api/bulk-data) with all the card.

- [MTGJson](https://mtgjson.com): for the card prices, we resource to MTGJson that provides a download link to all card prices that they collect from major stores in Europe and United States. Their guidlines and licesing are available [here](https://github.com/mtgjson/mtgjson).

> **NOTE:**
This is a unoffical project for academic purpose only and should not be used for monetary gain. It is not funded or endorsed by any company.

## Getting Started

In Airflow:
- `Airflow Version: 1.10.9`

- Create Connections:

    Scryfall: 
    ```
    Conn Id: scryfall_api
    Conn Type: HTTP
    Host: https://api.scryfall.com
    ```

    AWS:
    ```
    Conn Id: aws_credentials
    Conn Type: Amazon Web Services
    Login: your_user_access_key
    Password: your_user_secret_access_key
    ```

- Install libraries if not in the environment
    ``` sh
    pip install boto3
    pip install pandas
    ```
