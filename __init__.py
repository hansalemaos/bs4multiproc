import functools
import platform
import sys
import multiprocessing
import pickle
import subprocess
import tempfile
from functools import partial
import requests
from bs4 import SoupStrainer
from fake_headers import Headers
import itertools
from email import policy
from email.parser import BytesParser
from a_pandas_ex_apply_ignore_exceptions import pd_add_apply_ignore_exceptions
import pandas as pd
from flatten_any_dict_iterable_or_whatsoever import fla_tu

pd_add_apply_ignore_exceptions()
import os
import regex
import bs4
import lxml
import cchardet


limit = 10000
iswindows = "win" in platform.platform().lower()
if iswindows:
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    creationflags = subprocess.CREATE_NO_WINDOW
    invisibledict = {
        "startupinfo": startupinfo,
        "creationflags": creationflags,
        "start_new_session": True,
    }
    from ctypes import wintypes
    import ctypes

    windll = ctypes.LibraryLoader(ctypes.WinDLL)
    kernel32 = windll.kernel32

    _GetShortPathNameW = kernel32.GetShortPathNameW
    _GetShortPathNameW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
    ]
    _GetShortPathNameW.restype = wintypes.DWORD

else:
    invisibledict = {}


@functools.cache
def _get_soup_objects(so, ht):
    x = pickle.loads(so)
    if isinstance(x, type(None)):
        return bs4.BeautifulSoup(markup=ht, features="lxml")
    if len(x) == 0:
        return bs4.BeautifulSoup(markup=ht, features="lxml")
    return x


def get_fake_header():
    header = Headers(headers=False).generate()
    agent = header["User-Agent"]

    headers = {
        "User-Agent": f"{agent}",
    }

    return headers


def get_html_src(htmlcode, fake_header=True):
    if isinstance(htmlcode, str):
        if os.path.exists(htmlcode):
            if os.path.isfile(htmlcode):
                with open(htmlcode, mode="rb") as f:
                    htmlcode = f.read()
        elif regex.search(r"^.{1,10}://", str(htmlcode)) is not None:
            if not fake_header:
                htmlcode = requests.get(htmlcode).content
            else:
                htmlcode = requests.get(htmlcode, headers=get_fake_header()).content
        else:
            htmlcode = htmlcode.encode("utf-8", "backslashreplace")
    return htmlcode


@functools.cache
def instcheck(x):
    return isinstance(x, str)


def _get_pickle(sxstr, sx):
    try:
        dux = bs4.BeautifulSoup(
            markup=sxstr,
            parse_only=SoupStrainer(sx.name, attrs=sx.attrs, string=sx.text),
            features="lxml",
        )
    except Exception as e:
        sys.stderr.write(f"{e}")
        try:
            dux = bs4.BeautifulSoup(
                markup=sxstr, parse_only=sx.__dict__["parse_only"], features="lxml"
            )

        except Exception as e:
            sys.stderr.write(f"{e}")
            dux = None
    return pickle.dumps(dux, protocol=pickle.HIGHEST_PROTOCOL)


def soup_parsing(content, shared_list):
    sys.setrecursionlimit(limit)
    for i, sx in enumerate(
        bs4.BeautifulSoup(markup=content[1], features="lxml").find_all()
    ):
        t = tuple(fla_tu(sx.attrs))
        lent = len(t)
        sxstr = str(sx)
        shared_list.extend(
            [
                (
                    sx.name,
                    q[0],
                    list(itertools.takewhile(instcheck, q[1]))[0],
                    sx.text.strip(),
                    sxstr,
                    content[2],
                    content[0],
                    i,
                    lent,
                    i2,
                    _get_pickle(sxstr, sx),
                )
                for i2, q in enumerate(t)
            ]
        )


def get_tmpfile(suffix=".txt"):
    tfp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    filename = tfp.name
    filename = os.path.normpath(filename)
    tfp.close()
    return filename, partial(os.remove, tfp.name)


def parse_html_subprocess(html, chunks=2, processes=None):
    r"""
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

    """
    processes = get_procs(processes)
    sys.setrecursionlimit(limit)

    v = pickle.dumps(html, protocol=pickle.HIGHEST_PROTOCOL)

    fi, remo = get_tmpfile(suffix=".xxtmpxx")
    with open(fi, mode="wb") as f:
        f.write(v)
    p = subprocess.run(
        [sys.executable, __file__, fi, str(chunks), str(processes)],
        capture_output=True,
        **invisibledict,
    )
    filename = p.stdout.decode().strip()
    df = pd.read_pickle(filename)
    df["aa_soup"] = df.ds_apply_ignore(
        pd.NA, lambda x: _get_soup_objects(x["aa_soup"], x["aa_html"]), axis=1
    )
    _get_soup_objects.cache_clear()
    try:
        remo()
    except Exception:
        pass
    try:
        os.remove(filename)
    except Exception:
        pass
    return df


def get_procs(processes):
    if not processes:
        processes = os.cpu_count() - 1
    return processes


def multidata(htmls):
    allco = []

    for iht, html in enumerate(htmls):
        multipart_message = get_html_src(html)
        message = BytesParser(policy=policy.default).parsebytes(multipart_message)
        con = 0
        for part in message.walk():
            try:
                content = part.get_payload(decode=True)

                if content:
                    allco.append([con, content, iht])
                    con = con + 1

            except Exception as fe:
                sys.stderr.write(f"{fe}\n")
    return allco


@functools.cache
def _getsoup(aa_html, aa_soup):
    return bs4.BeautifulSoup(aa_html, features="lxml") if len(aa_soup) == 0 else aa_soup


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
    if isinstance(htmls, (str, bytes)):
        htmls = [htmls]
    htmls = multidata(htmls)

    htmls = [q for q in htmls if q and isinstance(q, list) and len(q) == 3]
    processes = get_procs(processes)
    with multiprocessing.Manager() as manager:
        shared_list = manager.list()
        with multiprocessing.Pool(processes=processes) as pool:
            pool.starmap(
                soup_parsing,
                ((value, shared_list) for value in htmls),
                chunksize=chunks,
            )
        d = (
            pd.DataFrame(
                [x for x in shared_list],
                columns=[
                    "aa_tag",
                    "aa_value",
                    "aa_attr",
                    "aa_text",
                    "aa_html",
                    "aa_h0",
                    "aa_h1",
                    "aa_h2",
                    "aa_h3",
                    "aa_h4",
                    "aa_soup",
                ],
            )
            .sort_values(
                by=[
                    "aa_h0",
                    "aa_h1",
                    "aa_h2",
                ]
            )
            .reset_index(drop=True)
            .assign(
                aa_soup=lambda h: h.apply(
                    lambda y: _get_soup_objects(y["aa_soup"], y["aa_html"]), axis=1
                )
            )
        )
    _getsoup.cache_clear()
    return d


def parse_html_multisub(htmls, chunks=2, processes=5):
    if isinstance(htmls, (str, bytes)):
        htmls = [htmls]
    processes = get_procs(processes)
    htmls = multidata(htmls)

    htmls = [q for q in htmls if q and isinstance(q, list) and len(q) == 3]
    with multiprocessing.Manager() as manager:
        shared_list = manager.list()
        with multiprocessing.Pool(processes=processes) as pool:
            pool.starmap(
                soup_parsing,
                ((value, shared_list) for value in htmls),
                chunksize=chunks,
            )
        d = (
            pd.DataFrame(
                [x for x in shared_list],
                columns=[
                    "aa_tag",
                    "aa_value",
                    "aa_attr",
                    "aa_text",
                    "aa_html",
                    "aa_h0",
                    "aa_h1",
                    "aa_h2",
                    "aa_h3",
                    "aa_h4",
                    "aa_soup",
                ],
            )
            .sort_values(
                by=[
                    "aa_h0",
                    "aa_h1",
                    "aa_h2",
                ]
            )
            .reset_index(drop=True)
        )
    return d


if __name__ == "__main__":
    sys.setrecursionlimit(limit)

    if len(sys.argv) == 4:
        fi = sys.argv[1]
        if fi.endswith(".xxtmpxx"):
            if os.path.exists(fi):
                chunks, processes = int(sys.argv[2]), int(sys.argv[3])

                fi2, remo2 = get_tmpfile(suffix=".pickldu")
                sys.stdout.write(fi2)

                with open(fi, mode="rb") as f:
                    data = f.read()
                datar = pickle.loads(data)
                df = parse_html_multisub(
                    htmls=datar,
                    chunks=int(sys.argv[2]),
                    processes=int(sys.argv[3]),
                )
                df.to_pickle(fi2)
