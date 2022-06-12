import datetime

from bs4 import BeautifulSoup as bs
import requests
import sqlite3
import pandas as pd

ENDPOINT = 'https://covidlive.com.au/'
DATABASE = 'covid_data.db'

def create_database() -> None:
    """Creates a basic sqlite database for storing scrapped data
    """
    with sqlite3.connect(DATABASE) as connection:
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data
                (   
                    As_At_DateTime DATETIME,
                    Country TEXT,
                    State TEXT,
                    Cases INT,
                    New_Cases INT,
                    Deaths INT,
                    New_Deaths INT
                )
            '''
            )

        connection.commit()

create_database()

def write_dataframe_to_sql(df:pd.DataFrame) -> None:
    """Writes Pandas Dataframe to SQLite Db

    Args:
        df (pd.DataFrame): Dataframe matching Table `data`
    """
    with sqlite3.connect(DATABASE) as connection:
        df.to_sql('data', connection, if_exists='append', index=True)

def get_response(url:str) -> str:
    """Get Web Request Response

    Args:
        url (str): URL to send Get Request

    Returns:
        str: InnerHTML of Response
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def parse_html_table_to_df(table:str) -> pd.DataFrame:
    """Extract the first dataframe from HTML Table string

    Args:
        table (str): HTML Table

    Returns:
        pd.DataFrame: Table as a DataFrame
    """
    return pd.read_html(str(table))[0]

def cleanse_int_datatypes(df: pd.DataFrame):
    numeric_cols = ['CASES', 'NEW_CASES', 'DEATHS', 'NEW_DEATHS']
    for col in numeric_cols:
        try: 
            df[col] = pd.to_numeric(df[col].str.replace('[^.0-9]', ''))
        except AttributeError:
            pass
    return df

def clean_and_join_dataframes(cases_df:pd.DataFrame, deaths_df:pd.DataFrame) -> pd.DataFrame:
    """Helper Function to clean and join dataframes

    Args:
        cases_df (pd.DataFrame): Cases Dataframe 
        deaths_df (pd.DataFrame): Deaths Dataframe

    Returns:
        pd.DataFrame: Dataframe joined on Index column `STATE`
    """
    
    # Drop Var column
    cases_df.drop(columns=['VAR'], inplace=True)
    deaths_df.drop(columns=['VAR'], inplace=True)
    # Rename Net
    cases_df.rename(columns={'NET': 'NEW_CASES'}, inplace=True)
    deaths_df.rename(columns={'NET': 'NEW_DEATHS'}, inplace=True)
    
    # Join datasets
    df = cases_df.set_index('STATE').join(deaths_df.set_index('STATE'), how='inner')
    
    # clean dataset
    df = cleanse_int_datatypes(df)
    df['Country'] = 'Australia'
    df['As_At_DateTime'] = datetime.datetime.now()

    return df

def main():
    inner_html = get_response(ENDPOINT)
    soup = bs(inner_html,  'html.parser')
    cases = soup.find(name='table', class_= 'CASES')
    deaths = soup.find(name='table', class_= 'DEATHS')

    df = clean_and_join_dataframes(parse_html_table_to_df(str(cases)), parse_html_table_to_df(str(deaths)))

    write_dataframe_to_sql(df)


if __name__ == '__main__':
    main()