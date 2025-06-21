# general
import re
import pandas as pd

# rich
from rich.table import Table
from rich.console import Console
from rich.panel import Panel

# custom
from esgpull.custom import api, fileops
from esgpull.models import File


class SearchResults:
    """
    A class to hold search results from the Esgpull API.
    It can be used to filter, sort, and manipulate the results.
    """

    def __init__(
        self,
        search_criteria: dict,
        meta_criteria: dict,
        config_path: str = None,
    ):
        self.search_criteria = search_criteria
        self.meta_criteria = meta_criteria
        self.top_n = meta_criteria.get(
            "top_n", None
        )  # Default to 10 if not specified
        self.search_results = []  # List to hold File objects
        self.results_df = (
            None  # DataFrame to hold results for further processing
        )
        self.results_df_top = None  # DataFrame for top N results
        self.search_results_fp = None
        self.fs = api.EsgpullAPI().esg.fs  # File system from Esgpull API

    def load_config(self, config_path: str) -> None:
        """Load search criteria and metadata from a YAML configuration file."""
        config = fileops.read_yaml(config_path)
        self.search_criteria = config.get("search_criteria", {})
        self.meta_criteria = config.get("meta_criteria", {})
        self.top_n = self.meta_criteria.get("top_n", None)
        self.search_criteria["limit"] = self.meta_criteria.get(
            "limit", 4
        )  # Default limit if not specified

    def do_search(self) -> None:
        """Perform a search using the provided criteria and populate results."""
        api_instance = api.EsgpullAPI()
        results = api_instance.search(criteria=self.search_criteria)
        self.results_df = pd.DataFrame(results)
        if "_sa_instance_state" in self.results_df.columns:
            self.results_df = self.results_df.drop(
                columns=["_sa_instance_state"]
            )
        if not self.results_df.empty:
            return self.sort_results_by_metadata()
        else:
            print("[SearchResults] No results found for given criteria.")

    def sort_results_by_metadata(self) -> None:
        """Sort a list of File objects by institution_id, source_id, experiment_id, member_id."""
        if self.results_df is None or self.results_df.empty:
            print("[SearchResults] No results to sort.")
            return
        # convert resolutions to float for sorting
        resolutions = self.results_df.apply(
            lambda f: self.calc_resolution(f.nominal_resolution), axis=1
        )
        self.results_df["nominal_resolution"] = resolutions
        self.results_df = self.results_df.sort_values(
            by=["nominal_resolution", "dataset_id"]
        )
        # Update self.search_results to match the sorted DataFrame
        self.search_results = [
            File(**{k: v for k, v in row.items() if k != "_sa_instance_state"})
            for _, row in self.results_df.iterrows()
        ]

    def calc_resolution(self, res) -> float:
        """
        Extract nominal resolution from file.nominal_resolution and return in degrees.
        Supports 'xx km', 'x x degree', or 'x degree'. Returns large value if unknown.
        Handles both string and numeric input.
        """
        if isinstance(res, (float, int)):
            return float(res)
        if not res:
            return 9999.0
        res = str(res).lower().replace(" ", "")
        if m := re.match(r"([\d.]+)km", res):
            return float(m.group(1)) / 111.0
        if m := re.match(r"([\d.]+)x([\d.]+)degree", res):
            return (float(m.group(1)) + float(m.group(2))) / 2.0
        if m := re.match(r"([\d.]+)degree", res):
            return float(m.group(1))
        return 9999.0

    def search_message(self, search_state: str) -> None:
        """Display a nicely formatted search message using rich."""
        console = Console()
        if search_state == "pre":
            table = Table(
                title="Search Criteria",
                show_header=True,
                header_style="bold magenta",
            )
            table.add_column("Key", style="dim", width=20)
            table.add_column("Value", style="bold")
            for k, v in self.search_criteria.items():
                table.add_row(str(k), str(v))
            console.print(
                Panel(
                    table, title="[cyan]Starting Search", border_style="cyan"
                )
            )
        elif search_state == "post":
            if len(self.search_results) == self.meta_criteria.get(
                "limit", None
            ):
                match_msg = (
                    " [orange1](limit of search search reached)[/orange1]"
                )
            else:
                match_msg = ""
            msg = f"[green]Search completed.[/green] [bold]{len(self.search_results)}[/bold] files{match_msg} found matching criteria."  # noqa
            console.print(
                Panel(msg, title="[green]Search Results", border_style="green")
            )

    def get_top_n(self) -> pd.DataFrame:
        """
        Return all files associated with the top n groups,
        where groups are defined by ['institution_id', 'source_id', 'experiment_id'].
        """
        if self.results_df is None:
            raise ValueError(
                "No results to select from. Run do_search() first."
            )

        top_dataset_ids = self.results_df.drop_duplicates("dataset_id").head(
            self.top_n
        )["dataset_id"]
        return self.results_df[
            self.results_df["dataset_id"].isin(top_dataset_ids)
        ]

    def clean_and_join_dict_vals(self):
        def clean_value(val):
            if isinstance(val, int):
                return str(val)
            if isinstance(val, str) and "," in val:
                # Split, strip, sort, join with no spaces
                items = sorted(map(str.strip, val.split(",")))
                return ",".join(items)
            if isinstance(val, str):
                return val.strip()
            return str(val)

        # Clean all values
        cleaned_str = [clean_value(v) for v in self.search_criteria.values()]
        # order alphabetically
        cleaned_str.sort()
        return "SEARCH_" + "_".join(cleaned_str).replace(" ", "")

    def save_searches(self) -> None:
        """Save the search results to a CSV file."""
        # check if search directory exists, if not create it
        search_dir = self.fs.auth.parent / "search_results"
        search_dir.mkdir(parents=True, exist_ok=True)
        self.search_id = self.clean_and_join_dict_vals()
        self.search_results_fp = search_dir / f"{self.search_id}.csv"
        if self.results_df is None:
            raise ValueError("No results to save. Run do_search() first.")

        if not self.search_results_fp.exists():
            self.results_df.to_csv(self.search_results_fp, index=False)
            print(f"Search results saved to {self.search_results_fp}")
        else:
            print(
                f"Search results already exist at {self.search_results_fp}. Not overwriting."
            )

    def load_search_results(self) -> pd.DataFrame:
        """Load search results from a CSV file."""
        search_dir = self.fs.auth.parent / "search_results"
        search_fp = search_dir / f"{self.search_id}.csv"
        if search_fp.exists():
            self.results_df = pd.read_csv(search_fp)
            if "_sa_instance_state" in self.results_df.columns:
                self.results_df = self.results_df.drop(
                    columns=["_sa_instance_state"]
                )
            self.search_results_fp = search_fp
            self.search_results = [
                File(
                    **{
                        k: v
                        for k, v in row.items()
                        if k != "_sa_instance_state"
                    }
                )
                for _, row in self.results_df.iterrows()
            ]
            return self.results_df
        else:
            raise FileNotFoundError(
                f"Search results file {search_fp} not found."
            )

    def run(self) -> list[File]:
        """Perform search, sort, and return top n results as File objects. Loads from cache if available, else performs
        search and saves."""
        if not self.search_criteria or not self.meta_criteria:
            self.load_config(
                fileops.read_yaml(fileops.REPO_ROOT / "search.yaml")
            )
        # Try to load from cache if available, else perform search and save
        try:
            self.search_id = self.clean_and_join_dict_vals()
            self.load_search_results()
            print(
                f"Loaded search results from cache: {self.search_results_fp}"
            )
            self.search_message("post")
        except Exception:
            self.search_message("pre")
            self.do_search()
            if self.results_df.empty:
                print("[SearchResults] No results found for given criteria.")
                return []
            self.search_message("post")
            self.sort_results_by_metadata()
            self.save_searches()
        # Always get top_n from the current results_df
        top_n_df = self.get_top_n() if self.top_n else self.results_df
        # limit
        if self.meta_criteria.get("limit", None):
            top_n_df = top_n_df.head(self.meta_criteria["limit"])
        return [File(**row) for _, row in top_n_df.iterrows()]
