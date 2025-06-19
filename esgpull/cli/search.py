from __future__ import annotations # Enables postponed evaluation of type hints

from datetime import datetime # Imports datetime for date handling

import click # Imports click for CLI creation
from click.exceptions import Abort, Exit # Imports specific click exceptions

from esgpull.cli.decorators import args, groups, opts # Imports CLI decorators
from esgpull.cli.utils import filter_keys, init_esgpull, parse_query, totable # Imports CLI utilities
from esgpull.exceptions import PageIndexError # Imports custom exception
from esgpull.graph import Graph # Imports Graph class for data representation
from esgpull.models import Query # Imports Query model
from esgpull.tui import Verbosity # Imports Verbosity enum for TUI


@click.command() # Defines a click command
@args.facets # Decorator for facets argument
@groups.query_def # Decorator for query definition options
@groups.query_date # Decorator for query date options
@groups.display # Decorator for display options
@groups.json_yaml # Decorator for JSON/YAML output options
@opts.detail # Decorator for detail option
@opts.no_default_query # Decorator for no_default_query option
@opts.show # Decorator for show option
@opts.dry_run # Decorator for dry_run option
@opts.file # Decorator for file option
@opts.facets_hints # Decorator for facets_hints option
@opts.hints # Decorator for hints option
@opts.yes # Decorator for yes option
@opts.record # Decorator for record option
@opts.verbosity # Decorator for verbosity option
def search( # Defines the search command function
    facets: list[str], # Facets to search for
    ## query_def
    tags: list[str], # Tags for the query
    require: str | None, # Required query string
    distrib: str | None, # Distributed search option
    latest: str | None, # Latest version option
    replica: str | None, # Replica option
    retracted: str | None, # Retracted option
    ## query_date
    date_from: datetime | None, # Start date for search
    date_to: datetime | None, # End date for search
    ## display
    _all: bool, # Display all results option
    zero: bool, # Display zero results option
    page: int, # Page number for results
    ## json_yaml
    json: bool, # Output in JSON format
    yaml: bool, # Output in YAML format
    ## ungrouped
    detail: int | None, # Detail level for results
    no_default_query: bool, # Disable default query
    show: bool, # Show query instead of searching
    dry_run: bool, # Perform a dry run
    file: bool, # Search for files instead of datasets
    facets_hints: bool, # Show facet hints
    hints: list[str] | None, # Specific hints to show
    yes: bool, # Auto-confirm prompts
    record: bool, # Record the command
    verbosity: Verbosity, # Verbosity level for TUI
) -> None:
    """
    Search datasets and files on ESGF

    More info
    """
    esg = init_esgpull( # Initializes esgpull
        verbosity, # Sets verbosity
        safe=False, # Disables safe mode
        record=record, # Enables recording if specified
        no_default_query=no_default_query, # Disables default query if specified
    )
    with esg.ui.logging("search", onraise=Abort): # Sets up logging for the search command
        query = parse_query( # Parses the query from CLI arguments
            facets=facets,
            tags=tags,
            require=require,
            distrib=distrib,
            latest=latest,
            replica=replica,
            retracted=retracted,
        )
        query.compute_sha() # Computes the SHA hash of the query
        esg.graph.resolve_require(query) # Resolves required queries in the graph
        query = esg.insert_default_query(query)[0] # Inserts the default query if not disabled
        if show: # If show option is enabled
            if json: # If JSON output is requested
                esg.ui.print(query.asdict(), json=True) # Prints query as JSON
            elif yaml: # If YAML output is requested
                esg.ui.print(query.asdict(), yaml=True) # Prints query as YAML
            else: # Otherwise, print in default format
                try:
                    graph = esg.graph.subgraph(query, parents=True) # Gets subgraph for the query
                    esg.ui.print(graph) # Prints the graph
                except KeyError: # If query not found in graph
                    esg.ui.print(query) # Prints the query directly
            esg.ui.raise_maybe_record(Exit(0)) # Exits after showing, possibly recording
        esg.graph.add(query, force=True) # Adds the query to the graph, forcing if necessary
        query = esg.graph.expand(query.sha) # Expands the query using its SHA from the graph
        hits = esg.context.hits( # Gets the number of hits for the query
            query,
            file=file, # Specifies whether to search for files
            date_from=date_from, # Specifies start date
            date_to=date_to, # Specifies end date
        )
        nb = sum(hits) # Calculates total number of hits
        page_size = esg.config.cli.page_size # Gets page size from config
        if detail is not None: # If detail level is specified
            page_size = 1 # Sets page size to 1 for detail view
            page = detail # Sets page to the detail index
        nb_pages = (nb // page_size) or 1 # Calculates number of pages
        offset = page * page_size # Calculates offset for pagination
        max_hits = min(page_size, nb - offset) # Calculates maximum hits for the current page
        if page > nb_pages: # If requested page is out of bounds
            raise PageIndexError(page, nb_pages) # Raises page index error
        elif zero: # If zero results option is enabled
            max_hits = 0 # Sets max hits to 0
        elif _all: # If all results option is enabled
            offset = 0 # Resets offset to 0
            max_hits = nb # Sets max hits to total number of hits
        ids = range(offset, offset + max_hits) # Generates a range of IDs for the current page
        if dry_run: # If dry run option is enabled
            search_results = esg.context.prepare_search( # Prepares search requests without executing
                query,
                file=file,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                date_from=date_from,
                date_to=date_to,
            )
            for result in search_results: # Iterates over prepared search requests
                esg.ui.print(result.request.url) # Prints the request URL
            esg.ui.raise_maybe_record(Exit(0)) # Exits after dry run, possibly recording
        if facets_hints: # If facets hints option is enabled
            not_distrib_query = query << Query(options=dict(distrib=False)) # Creates a query without distributed search
            facet_counts = esg.context.hints( # Gets facet counts
                not_distrib_query,
                file=file,
                facets=["*"], # Gets all facets
                date_from=date_from,
                date_to=date_to,
            )
            esg.ui.print(list(facet_counts[0]), json=True) # Prints facet counts as JSON
            esg.ui.raise_maybe_record(Exit(0)) # Exits after showing hints, possibly recording
        if hints is not None: # If specific hints are requested
            facet_counts = esg.context.hints( # Gets facet counts for specified hints
                query,
                file=file,
                facets=hints,
                date_from=date_from,
                date_to=date_to,
            )
            esg.ui.print(facet_counts, json=True) # Prints facet counts as JSON
            esg.ui.raise_maybe_record(Exit(0)) # Exits after showing hints, possibly recording
        if max_hits > 200 and not yes: # If many hits and not auto-confirmed
            nb_req = max_hits // esg.config.api.page_limit # Calculates number of requests
            message = f"{nb_req} requests will be sent to ESGF. Send anyway?" # Confirmation message
            if not esg.ui.ask(message, default=True): # Asks for confirmation
                esg.ui.raise_maybe_record(Abort) # Aborts if not confirmed
        if detail is not None: # If detail level is specified
            queries = esg.context.search_as_queries( # Searches and returns results as queries
                query,
                file=file,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                keep_duplicates=True,
                date_from=date_from,
                date_to=date_to,
            )
            if json: # If JSON output is requested
                selections = [q.selection.asdict() for q in queries] # Extracts selections as dicts
                esg.ui.print(selections, json=True) # Prints selections as JSON
            elif yaml: # If YAML output is requested
                selections = [q.selection.asdict() for q in queries] # Extracts selections as dicts
                esg.ui.print(selections, yaml=True) # Prints selections as YAML
            else: # Otherwise, print in default format
                graph = Graph(None) # Creates a new graph
                graph.add(*queries, clone=False) # Adds queries to the graph
                esg.ui.print(graph) # Prints the graph
            esg.ui.raise_maybe_record(Exit(0)) # Exits after showing details, possibly recording
        results = esg.context.search( # Performs the actual search
            query,
            file=file,
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            keep_duplicates=True,
            date_from=date_from,
            date_to=date_to,
        )
        if json: # If JSON output is requested
            esg.ui.print([f.asdict() for f in results], json=True) # Prints results as JSON
        elif yaml: # If YAML output is requested
            esg.ui.print([f.asdict() for f in results], yaml=True) # Prints results as YAML
        else: # Otherwise, print in default format
            f_or_d = "file" if file else "dataset" # Determines if searching for files or datasets
            s = "s" if nb != 1 else "" # Adds plural 's' if needed
            esg.ui.print(f"Found {nb} {f_or_d}{s}.") # Prints number of found items
            if results: # If there are results
                unique_ids = {r.master_id for r in results} # Gets unique master IDs
                unique_nodes = {(r.master_id, r.data_node) for r in results} # Gets unique master ID and data node pairs
                needs_data_node = len(unique_nodes) > len(unique_ids) # Checks if data node is needed for uniqueness
                docs = filter_keys(results, ids=ids, data_node=needs_data_node) # Filters keys for display
                esg.ui.print(totable(docs)) # Prints results as a table
        esg.ui.raise_maybe_record(Exit(0)) # Exits after search, possibly recording

