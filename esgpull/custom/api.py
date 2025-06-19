from pathlib import Path
from typing import Any, Optional, List, Dict
from esgpull.esgpuller import Esgpull
from esgpull.cli.utils import (
    parse_query,
    serialize_queries_from_file,
)
from esgpull.tui import Verbosity
from esgpull.graph import Graph
from esgpull.models import Query
from esgpull.models import File

# custom
from esgpull.custom import search, download, regrid, fileops, api


class EsgpullAPI:
    """
    Python API for interacting with esgpull, using the same logic as the CLI scripts.
    """

    def __init__(
        self,
        config_path: Optional[str | Path] = None,
        verbosity: str = "detail",
    ):
        self.verbosity = Verbosity[verbosity.capitalize()]
        self.esg = Esgpull(path=config_path, verbosity=self.verbosity)

    def search(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Searches ESGF nodes for files/datasets matching the criteria.

        Args:
            criteria: A dictionary of search facets (e.g., project, variable).
                      Can include 'limit' to restrict the number of results.

        Returns:
            A list of dictionaries, where each dictionary represents a found file/dataset.
        """
        # Use the same logic as CLI: parse_query, then context.search
        _criteria = criteria.copy()
        max_hits = _criteria.pop("limit", None)
        tags = _criteria.pop("tags", [])
        query = parse_query(
            facets=[f"{k}:{v}" for k, v in _criteria.items()],
            tags=tags,
            require=None,
            distrib=None,
            latest=None,
            replica=None,
            retracted=None,
        )
        query.compute_sha()
        self.esg.graph.resolve_require(query)
        results = self.esg.context.search(query, file=True, max_hits=max_hits)
        return [r.asdict() for r in results]

    def add(self, criteria: Dict[str, Any], track: bool = False) -> None:
        """
        Adds or updates a query in the esgpull database.

        Args:
            criteria: A dictionary defining the query. Must include a unique 'name'.
                      Other keys can be facets (e.g., project, variable), 'limit', or 'tags'.
            track: If True, the query will be marked for tracking.
        """
        # Use the same logic as CLI add.py
        _criteria = criteria.copy()
        query_file = _criteria.pop("query_file", None)
        tags = _criteria.pop("tags", [])
        facets = [f"{k}:{v}" for k, v in _criteria.items() if k != "name"]
        name = _criteria.get("name")
        queries = []
        if query_file is not None:
            queries = serialize_queries_from_file(Path(query_file))
        else:
            query = parse_query(
                facets=facets,
                tags=tags + [f"name:{name}"] if name else tags,
                require=None,
                distrib=True,
                latest=None,
                replica=None,
                retracted=None,
            )
            self.esg.graph.resolve_require(query)
            if track:
                query.track(query.options)
            queries = [query]
        queries = self.esg.insert_default_query(*queries)
        empty = Query()
        empty.compute_sha()

        for query in queries:
            query.compute_sha()
            self.esg.graph.resolve_require(query)
            if query.sha == empty.sha:
                raise ValueError("Trying to add empty query.")
            if query.sha in self.esg.graph:
                print("Skipping existing query:", query.name)
                continue
            else:
                self.esg.graph.add(query)
                print("New query added:", query.name)
        new_queries = self.esg.graph.merge()
        if new_queries:
            print(
                f"{len(new_queries)} new query{'s' if len(new_queries) > 1 else ''} added."
            )
        else:
            print("No new query was added.")

    def valid_name_tag(
        self,
        graph: Graph,
        query_id: str | None,
        tag: str | None,
    ) -> bool:
        result = True
        # get query from id

        if query_id is not None:
            shas = graph.matching_shas(query_id, graph._shas)
            if len(shas) > 1:
                print("Multiple matches found for query ID:", query_id)
                result = False
            elif len(shas) == 0:
                print("No matches found for query ID:", query_id)
                result = False
        elif tag is not None:
            tags = [t.name for t in graph.get_tags()]
            if tag not in tags:
                print("No queries tagged with:", tag)
                result = False
        return result

    def track_query(self, query_ids: list[str]) -> None:
        """
        Marks existing queries for automatic tracking.

        Args:
            query_ids: List of query names or SHAs to track.
        """
        for sha in query_ids:
            if not self.valid_name_tag(self.esg.graph, sha, None):
                print(
                    f"Invalid query ID: {sha}. Are you using a pre-tracked query_id?"
                )
                continue
            query = self.esg.graph.get(sha)
            if query.tracked:
                print(f"{query.name} is already tracked.")
                continue
            if self.esg.graph.get_children(query.sha):
                print(
                    "This query has children"
                )  # TODO: handle children logic if needed
            expanded = self.esg.graph.expand(query.sha)
            tracked_query = query.clone(compute_sha=False)
            tracked_query.track(expanded.options)
            tracked_query.compute_sha()
            if tracked_query.sha == query.sha:
                # No change in SHA, just mark as tracked and merge
                query.tracked = True
                self.esg.graph.merge()
                self.esg.ui.print(f":+1: {query.name}  is now tracked.")
            else:
                self.esg.graph.replace(query, tracked_query)
                self.esg.graph.merge()
                # self.esg.ui.print(f":+1: {tracked_query.name} (previously {query.name}) is now tracked.")
                print(
                    f":+1: {tracked_query.name} (previously {query.name}) is now tracked."
                )
            return

    def untrack_query(self, query_id: str) -> None:
        """
        Unmarks an existing query from automatic tracking.

        Args:
            query_id: The name of the query to untrack.
        """
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        untracked_query = query.clone()
        untracked_query.tracked = False
        self.esg.graph.replace(query, untracked_query)
        self.esg.graph.merge()

    def update(
        self, query_id: str, subset_criteria: Optional[dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Updates a tracked query by searching for matching files on ESGF,
        adds new files to the database, and returns their information.

        Args:
            query_id: The name or SHA of the query to update.
            subset_criteria: Optional dict to further filter files (facet:value).

        Returns:
            A list of dictionaries representing new files added to the query.
        """
        self.esg.graph.load_db()
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        if not getattr(query, "tracked", False):
            raise ValueError(
                f"Query '{query.name}' is not tracked. Only tracked queries can be updated."
            )

        expanded = self.esg.graph.expand(query.sha)
        # Search for files matching the expanded query
        files_found = self.esg.context.search(expanded, file=True)
        # Get SHAs of files already linked to the query
        existing_shas = {
            getattr(f, "sha", None) for f in getattr(query, "files", [])
        }
        # Filter out files already linked
        new_files = []
        for file in files_found:
            if getattr(file, "sha", None) not in existing_shas:
                if subset_criteria and not all(
                    getattr(file, k, None) == v
                    for k, v in subset_criteria.items()
                ):
                    continue
                new_files.append(file)
        # Link new files to the query in the DB
        if new_files:
            for file in new_files:
                file.status = (
                    getattr(file, "status", None) or File.Status.Queued
                )
                self.esg.db.session.add(file)
                self.esg.db.link(query=query, file=file)
            query.updated_at = self.esg.context.now()
            self.esg.db.session.add(query)
            self.esg.db.session.commit()
        self.esg.graph.merge()
        return [f.asdict() for f in new_files]

    def download(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Downloads files associated with a given query that are in 'queued' state.

        Args:
            query_id: The name of the query for which to download files.

        Returns:
            A list of dictionaries representing the files processed by the download command,
            including their status.
        """
        # Use the same logic as CLI download.py (simplified)
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        files_to_download = self.esg.db.get_files_by_query(query, status=None)
        if not files_to_download:
            return []
        downloaded_files, error_files = self.esg.sync(
            self.esg.download(files_to_download, show_progress=False)
        )
        all_processed_files = downloaded_files + [
            err.data for err in error_files if hasattr(err, "data")
        ]
        return [f.asdict() for f in all_processed_files]

    def list_queries(self) -> List[Dict[str, Any]]:
        """
        Lists all queries in the esgpull database.

        Returns:
            A list of dictionaries, where each dictionary represents a query.
        """
        self.esg.graph.load_db()  # load queries from db (otherwise inaccessible)
        return [
            {"name": q.name, "sha": q.sha, **q.asdict()}
            for q in self.esg.graph.queries.values()
        ]

    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a specific query by its name.

        Args:
            query_id: The name of the query.

        Returns:
            A dictionary representing the query if found, otherwise None.
        """
        self.esg.graph.load_db()
        query = self.esg.graph.get(name=query_id)
        if query:
            return {"name": query.name, "sha": query.sha, **query.asdict()}
        return None


def run():
    REPO_ROOT = fileops.get_repo_root()
    CRITERIA_FP = fileops.read_yaml(REPO_ROOT / "search.yaml")
    SEARCH_CRITERIA_CONFIG = CRITERIA_FP.get("search_criteria", {})
    META_CRITERIA_CONFIG = CRITERIA_FP.get("meta_criteria", {})
    API = api.EsgpullAPI()

    # load configuration
    files = search.SearchResults(
        search_criteria=SEARCH_CRITERIA_CONFIG,
        meta_criteria=META_CRITERIA_CONFIG,
    ).run()

    download.DownloadSubset(
        files=files,
        fs=API.esg.fs,
        output_dir=fileops.META_CRITERIA_CONFIG.get(
            "output_dir", None
        ),  # Optional output directory
        subset=fileops.META_CRITERIA_CONFIG.get(
            "subset"
        ),  # Optional subset criteria for xarray
        max_workers=fileops.META_CRITERIA_CONFIG.get(
            "max_workers", 32
        ),  # Default to 16 workers
    ).run()
    if fileops.META_CRITERIA_CONFIG.get("regrid", False):
        # if regrid is enabled, run regridding
        print("Regridding enabled, running regridding...")
        # create a RegridderManager instance
        regrid.RegridderManager(
            fs=API.esg.fs,
            watch_dir=API.esg.fs.data,
        )


if __name__ == "__main__":
    run()
