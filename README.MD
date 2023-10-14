# BeautifulSoup multiprocessing parsing to pandas DataFrame 

## Tested against Windows / Python 3.11 / Anaconda

## pip install bs4multiproc


A Python library for parsing HTML content. It leverages various libraries and methods, including BeautifulSoup, pandas, multiprocessing, and subprocesses, to efficiently extract structured information from HTML documents. 

## What does it do?

### HTML Parsing: 

The library's primary purpose is to parse HTML content. 
It can handle both local HTML files and web-based 
HTML content retrieved via URLs.

### Parallel Processing: 

The library offers two main functions, parse_html_subprocess and parse_html_multiproc, to process HTML content in parallel. 
This parallelism can significantly speed up the parsing 
of multiple HTML documents.

### DataFrame Output: 

The library returns structured data in the form of pandas DataFrames. These DataFrames contain detailed information about HTML 
elements within the parsed content, such as tag names, 
attributes, text, and more.

### Caching: 

The script utilizes functools.cache for memoization, 
which can improve performance by avoiding 
unnecessary recomputation of previously processed data.

## Advantages of the Library:

### Efficiency: 

Parallel processing is a key advantage of this library. 
It can distribute the parsing of multiple HTML documents 
across multiple CPU cores, making it significantly faster 
when dealing with a large number of documents.

### Structured Data: 

The library doesn't just parse HTML; it structures the data 
in a tabular format using DataFrames. 
This structured data can be easily analyzed, 
transformed, and used for various purposes.

### Flexibility: 

The library is flexible and can handle various input 
sources, including local files, web URLs, 
and multipart messages (e.g., email content).

### Subprocess Support: 

The parse_html_subprocess function allows you to offload the HTML 
parsing task to a separate subprocess. This can be useful when 
dealing with potentially untrusted or resource-intensive 
HTML content, as it isolates the parsing process.

### Parallelism Control: 

You can control the level of parallelism by specifying 
the number of processes and chunks. 
This flexibility allows you to fine-tune the 
performance based on your system's capabilities and specific requirements.

### Caching: 

The caching mechanism helps save time by reusing previously 
parsed results, especially when working with the same 
content repeatedly.

### Cross-Platform: 

The library supports both Windows and non-Windows environments, 
ensuring compatibility across different operating systems.


## parse_html_subprocess

```python

def parse_html_subprocess(html,chunks=2,processes=None):
	Parse HTML Content Using Subprocess

	This function takes a single HTML content as input, processes it using a subprocess,
	and returns a structured DataFrame containing information about HTML elements.
	It is suitable for parsing a single HTML document using subprocess-based parallelism.

	Parameters:
	- html (str or bytes): HTML content to be processed. It can be provided as a string, bytes, or a file path.
	- chunks (int, optional): The number of chunks to divide the HTML processing into.
	This can help optimize processing for large datasets. Default is 2.
	- processes (int, optional): The number of parallel processes to use for parsing.
	If not specified, it defaults to (number of CPU cores - 1).

	Returns:
	- pandas.DataFrame: A DataFrame containing information about HTML elements, such as tag names, attributes, text, and more.


```


## parse_html_multiproc

```python

def parse_html_multiproc(htmls, chunks=2, processes=5):
    r"""
    Parse HTML Content Using Multiprocessing

    This function takes a list of HTML content, processes it in parallel using the multiprocessing library,
    and returns a structured DataFrame containing information about HTML elements. It is suitable for
    parsing multiple HTML documents simultaneously.

    Parameters:
    - htmls (list): A list of HTML content to be processed. Each item in the list should represent HTML content, typically as strings or bytes.
    - chunks (int, optional): The number of chunks to divide the HTML processing into. This can help optimize processing for large datasets. Default is 2.
    - processes (int, optional): The number of parallel processes to use for parsing. If not specified, it defaults to (number of CPU cores - 1).

    Returns:
    - pandas.DataFrame: A DataFrame containing information about HTML elements, such as tag names, attributes, text, and more.
    """
```

## Examples 

```python
import re
import sys

import bs4
from PrettyColorPrinter import add_printer
from bs4multiproc import parse_html_subprocess, parse_html_multiproc
import pandas as pd

add_printer(1)
from time import perf_counter

sys.setrecursionlimit(10000)
import numpy as np

if __name__ == "__main__":
    execute_examples = False
    if execute_examples:
        start = perf_counter()
        df1 = parse_html_multiproc(  # needs if __name__ == "__main__": !!!!
            htmls=[
                r"C:\Users\hansc\Downloads\bet365 - Apostas Desportivas Online.mhtml",
                "https://docs.python.org/3/library/multiprocessing.html",
                r"C:\Users\hansc\Downloads\Your Repositories.mhtml",
            ],
            chunks=1,
            processes=4,
        )
        end = perf_counter() - start

        start1 = perf_counter()
        df2 = parse_html_subprocess(  # doesn't need if __name__ == "__main__":
            html=[
                r"C:\Users\hansc\Downloads\bet365 - Apostas Desportivas Online.mhtml",
                "https://docs.python.org/3/library/multiprocessing.html",
                r"C:\Users\hansc\Downloads\Your Repositories.mhtml",
            ],
            chunks=1,
            processes=4,
        )
        end1 = perf_counter() - start1

        print(df1)
        print(df2)
        print(end, end1)
        df1.drop_duplicates(subset=["aa_h0", "aa_h1", "aa_h2", "aa_h3"]).aa_soup.apply(
            lambda x: g if (g := x.find_all("a")) else pd.NA
        ).dropna().ds_color_print_all()

    df = parse_html_multiproc(
        r"C:\Users\hansc\Downloads\bet365 - Apostas Desportivas Online2.mhtml",
        chunks=3,
        processes=4,
    )

    results = (
        df.loc[
            np.all(
                df[["aa_tag", "aa_value", "aa_attr"]].__array__()
                == np.array([["div", "ovm-Fixture-media", "class"]]),
                axis=1,
            )
        ]
        .aa_html.apply(
            lambda x: [
                [y.text]
                for y in bs4.BeautifulSoup(x).find_all(
                    re.compile(r"\b(?:span|div)\b"),
                    class_=re.compile(
                        "(?:ovm-ParticipantOddsOnly_Odds)|(?:ovm-FixtureDetailsTwoWay_TeamName)"
                    ),
                )
            ]
        )
        .apply(lambda q: [t[0] for t in q])
        .apply(pd.Series)
    ).reset_index(drop=True)
    print(results.to_string())
sys.setrecursionlimit(3000)

# Example - Odds - Live Games from bet365.com
#                               0                              1      2     3      4
# 0                      Criciúma                    Chapecoense   4.00  3.00   2.05
# 1                      Barbados             Dominican Republic    NaN   NaN    NaN
# 2             Trindade e Tobago                      Guatemala  11.00  4.75   1.33
# 3         CA Union Villa Krause              San Lorenzo Ullum   3.20  2.62   2.50
# 4                   Once Caldas            Jaguares de Córdoba   1.66  3.20   6.50
# 5             New Mexico United                 Memphis 901 FC   1.11  7.50  13.00
# 6                   FC Santiago           Huracanes Izcalli FC   2.75  3.60   2.20
# 7                Grupo Sherwood       Club Leones Huixquilucan   4.75  3.75   1.66
# 8                 Auckland City             Cashmere Technical   1.44  4.00   7.00
# 9                     Petone FC             Auckland United FC  12.00  8.00   1.11
# 10                  Árabe Unido         Sporting San Miguelito   3.50  2.30   2.75
# 11                    Udelas FC                    Union Cocle   3.50  1.61   5.50
# 12          Deportivo Maldonado                        Peñarol  29.00  5.00   1.18
# 13              Central Espanol                        Basanez   3.75  1.66   4.33
# 14     Argentina (JKey) Esports       Portugal (RuBIX) Esports   2.10  3.75   2.87
# 15  Eintracht (Aleksis) Esports  Dortmund (Kalibrikon) Esports   4.50  1.90   2.87
# 16   Germany (lowheels) Esports   France (DangerDim77) Esports   1.83  3.75   3.50
# 17          Lazio (Nio) Esports        Arsenal (Panic) Esports   1.80  5.00   3.00
# 18       Lens (General) Esports      Sevilla (Chemist) Esports   1.83  5.00   2.87
```