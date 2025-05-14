from pathlib import Path
from typing import Any, Optional, List, Dict

from esgpull.esgpull import Esgpull
from esgpull.models import Query, FileStatus, Options, Tag
from esgpull.result import Err

class EsgpullAPI:
    """
    Python API for interacting with esgpull.
    """

    def __init__(self, config_path: Optional[str | Path] = None):
        """
        Initializes the EsgpullAPI.

        Args:
            config_path: Optional path to the esgpull configuration file.
                         If None, esgpull will search in default locations.
        """
        self.esg = Esgpull(path=config_path)

    def search(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Searches ESGF nodes for files/datasets matching the criteria.

        Args:
            criteria: A dictionary of search facets (e.g., project, variable).
                      Can include 'limit' to restrict the number of results.

        Returns:
            A list of dictionaries, where each dictionary represents a found file/dataset.
        """
        _criteria = criteria.copy()
        max_hits = _criteria.pop("limit", None)

        # Convert single string values in selection to list of strings
        selection_dict = {}
        for k, v in _criteria.items():
            if isinstance(v, str):
                selection_dict[k] = [v]
            elif isinstance(v, list):
                selection_dict[k] = v
            else: # Attempt to convert to string and wrap in list
                selection_dict[k] = [str(v)]


        query = Query(selection=selection_dict)
        
        # Assuming search is for files by default, as per notebook context
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
        _criteria = criteria.copy()
        query_name_from_api = _criteria.pop("name", None)
        if not query_name_from_api:
            raise ValueError("'name' is required in criteria for add")

        query_limit = _criteria.pop("limit", None) # This limit is popped but will not be used further in this method.
        user_tags = _criteria.pop("tags", []) # Allow user to pass other tags

        # Remaining items in _criteria are for selection
        selection_dict = {}
        for k, v in _criteria.items():
            if isinstance(v, str):
                selection_dict[k] = [v]
            elif isinstance(v, list):
                selection_dict[k] = v
            else: # Attempt to convert to string and wrap in list
                selection_dict[k] = [str(v)]

        query_obj = Query(selection=selection_dict)

        # Manage tags:
        # Collect all desired tag names as strings
        tag_names_to_ensure = set(user_tags)
        tag_names_to_ensure.add(f"name:{query_name_from_api}")

        final_tags_for_query = []
        for name_str in sorted(list(tag_names_to_ensure)):
            # Check if a tag with this name already exists in the graph/DB
            tag_instance = self.esg.graph.get_tag(name=name_str)
            if tag_instance is None:
                # If not, create a new Tag object.
                # Its SHA will be computed either by Graph.add or the before_insert listener in tag.py.
                tag_instance = Tag(name=name_str)
            final_tags_for_query.append(tag_instance)
        
        query_obj.tags = final_tags_for_query # Assign the list of (potentially mixed new/existing) Tag objects

        self.esg.graph.add(query_obj, force=True)  # force=True to update if exists
        if track:
            # compute_sha might be needed if not done automatically before tracking
            if not query_obj.sha:
                 query_obj.compute_sha() # Or however SHA is computed/set
            self.esg.db.track(query_obj.sha, status=True)
        self.esg.graph.merge()  # Removed commit=True

    def update(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Updates a query by fetching the latest file information from ESGF nodes.

        Args:
            query_id: The name of the query to update.

        Returns:
            A list of dictionaries representing all files associated with the query after the update.
        """
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")

        updated_files_map = self.esg.sync(self.esg.update_queues([query]))
        files_for_query = updated_files_map.get(query.sha, [])
        return [f.asdict() for f in files_for_query]

    def download(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Downloads files associated with a given query that are in 'queued' state.

        Args:
            query_id: The name of the query for which to download files.

        Returns:
            A list of dictionaries representing the files processed by the download command,
            including their status.
        """
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")

        files_to_download = self.esg.db.get_files_by_query(query, status=FileStatus.Queued)

        if not files_to_download:
            return []

        downloaded_files, error_files_err_type = self.esg.sync(
            self.esg.download(files_to_download, show_progress=False) # show_progress=False for API
        )
        
        all_processed_files = downloaded_files
        for err in error_files_err_type:
            if isinstance(err, Err) and hasattr(err.data, 'asdict'):
                all_processed_files.append(err.data)
            # else: log or handle unexpected error type if necessary

        return [f.asdict() for f in all_processed_files]

    def track_query(self, query_id: str) -> None:
        """
        Marks an existing query for automatic tracking.

        Args:
            query_id: The name of the query to track.
        """
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        
        self.esg.db.track(query.sha, status=True)
        self.esg.graph.merge()  # Removed commit=True

    def untrack_query(self, query_id: str) -> None:
        """
        Unmarks an existing query from automatic tracking.

        Args:
            query_id: The name of the query to untrack.
        """
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")

        self.esg.db.track(query.sha, status=False)
        self.esg.graph.merge()  # Removed commit=True

    def list_queries(self) -> List[Dict[str, Any]]:
        """
        Lists all queries in the esgpull database.

        Returns:
            A list of dictionaries, where each dictionary represents a query.
        """
        all_queries = self.esg.graph.queries.values()
        return [q.asdict() for q in all_queries]

    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a specific query by its name.

        Args:
            query_id: The name of the query.

        Returns:
            A dictionary representing the query if found, otherwise None.
        """
        query = self.esg.graph.get(name=query_id)
        if query:
            return query.asdict()
        return None
