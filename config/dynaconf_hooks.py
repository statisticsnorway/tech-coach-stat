from dynaconf import Dynaconf


def post(settings: Dynaconf) -> dict[str, str]:
    """Set the GCP_PROJECT_ID based on the environment, prod or test."""
    dapla_team = settings.dapla_team
    env = settings.environment if settings.environment else None

    if env == "test":
        project = f"{dapla_team}-t-af"
    else:
        project = f"{dapla_team}-p-mb"  # default to prod behavior

    return {"gcp_project_id": project}
