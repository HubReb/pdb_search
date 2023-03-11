#!/usr/bin/env python3

from pylatexenc.latex2text import LatexNodes2Text
from pybtex.database import parse_file

from collections import defaultdict
from typing import List


def get_data(filename: str = None) -> dict:
    """
    Read latex data and construct dictionary of form
    {title : {contents: content in one sentence}, {bibtex: {bibtex identifier}}

    :param filename: name of latex file
    :return: dictionary of above form
    """
    if not filename:
        filename = input("Enter filename: ")
    assert filename.endswith("tex")
    with open(filename, encoding="utf-8") as f:
        data = f.read()
    text = LatexNodes2Text().latex_to_text(data).split("\n")
    text = [line for line in text if not line == ""]
    papers_dict = defaultdict(lambda: defaultdict(str))
    title, bibtex = None, None
    for line in text:
        if "*" in line and "<cit.>" in line:  # title case
            title = line.split("<cit.>")[1].rstrip(":")
            if title == ":":
                title = line.split("<cit.>")[0].split("*")[1]
            title = title.strip()
            for latex_line in data.split("\n"):
                if title in latex_line:
                    line_split = latex_line.split(r"\cite{")
                    if "\\item" in line_split[0]:
                        bibtex = line_split[1]
                    else:
                        bibtex = line_split[0]
                    bibtex = bibtex.split("}")[0]
                    break
            description = None
        else:
            description = line.strip()
        if description and title:
            papers_dict[title]["bibtex_id"] = bibtex
            papers_dict[title]["contents"] = description
            title, bibtex = None, None
    return papers_dict


def get_bibtex_information(papers_dict: dict, bib_somy: str) -> dict:
    """
    Read bib information from file, replace bibtex key with bibtex information and add authors.

    :param papers_dict: dictionary of the form
        {title: {bibtex: bibtex identifier}, {contents: content description}}
    :param bib_somy: file that contains the bibtex information
    :return: dictionary of the form
        {
            title: {bibtex: bibtex identifier}, {contents: description},
            {author: list of individual author names}
        }
    """
    bib_graph = parse_file(bib_somy, bib_format="bibtex")
    for i in bib_graph.entries:
        for entry in papers_dict.keys():
            if papers_dict[entry]["bibtex_id"] == i:
                authors_list = []
                for author in bib_graph.entries[i].persons["author"]:
                    authors_list.append(
                        f"{author.last_names[0]}, {author.first_names[0]}"
                    )
                papers_dict[entry]["bibtex"] = bib_graph.entries[i].to_string("bibtex")
                papers_dict[entry]["author"] = authors_list
                break
    return papers_dict


def get_single_bibtex_information(bibsomy: str) -> tuple[str, List[str], str, str]:
    """
    Read bib information from file

    :param bibsomy: file that contains the bibtex information
    :return: bibtex entry (str), authors (list), bibtex identifier (str)
    """
    bib_graph = parse_file(bibsomy, bib_format="bibtex")
    assert len(bib_graph.entries.keys()) == 1
    i = ""
    bibtex_data = ""
    bibtex_ident = ""
    for i in bib_graph.entries.keys():
        bibtex_data = bib_graph.entries[i].to_string("bibtex")
        bibtex_ident: str = i
    authors_list = []
    for author in bib_graph.entries[i].persons["author"]:
        authors_list.append(f"{author.last_names[0]}, {author.first_names[0]}")
    return (
        bibtex_data,
        authors_list,
        bibtex_ident,
        bib_graph.entries[bibtex_ident].fields["title"],
    )


def iterate_through_papers(papers: List[List[str]]) -> List[List[str]]:
    id_bib = papers[0][3]
    down_papers = []
    authors = ""
    for i, paper in enumerate(papers):
        paper = list(paper)  # papers element are tuples
        if id_bib != paper[3]:
            down_papers.append([authors[:-5]] + list(papers[i - 1])[1:])
            id_bib = paper[3]
        else:
            authors += f"{paper[0]} and "
    return down_papers


def get_user_choice(results: List) -> List:
    print("Following papers found: ")
    for i, paper in enumerate(results):
        print(f"{i + 1}: title: {paper[3]}")
    chosen_paper = -1
    while chosen_paper < 0 or chosen_paper >= len(results):
        chosen_paper = cast(input("Choose paper to extract: ")) - 1
        if chosen_paper < 0 or chosen_paper >= len(results):
            print("Please choose a valid number.")
    return results[chosen_paper]


def pretty_print_results(bibtex_data: List, paper_data: List):
    print(f"title: {paper_data[2]}\nauthors: {paper_data[0]}")
    print(f"summary: {paper_data[4]}\nbib entry: {bibtex_data[1]}")


def cast(user_input: str) -> int:
    """Check if user input is valid and cast to Integer if so"""
    try:
        cast_to_int = int(user_input)
    except ValueError:
        return -1
    return cast_to_int
