from pathlib import Path


def get_repo_root():
    return Path(__file__).resolve().parent.parent.parent


def read_yaml(file_path):
    import yaml

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


REPO_ROOT = get_repo_root()
CRITERIA_FP = read_yaml(REPO_ROOT / "search.yaml")
SEARCH_CRITERIA_CONFIG = CRITERIA_FP.get("search_criteria", {})
META_CRITERIA_CONFIG = CRITERIA_FP.get("meta_criteria", {})
