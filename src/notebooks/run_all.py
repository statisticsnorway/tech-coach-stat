# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#   kernelspec:
#     display_name: tech-coach-stat
#     language: python
#     name: tech-coach-stat
# ---

# %% [markdown]
# # Kjøring av hele produksjonsløpet
# Denne notebook'en kjører hele produksjonsløpet fra datainnhenting til ferdige utdata.
# Den baserer seg på kalle funksjonen `run_all()` på alle modulene i rekkefølge.
# Kommenter ut de bitene du vil at den ikke skal kjøre.

# %%
import logging

import a_collect_data
import b_kildomat_local
import c_pre_inndata_to_inndata
import d_prepare_edit
from fagfunksjoner.log.statlogger import StatLogger

from config.config import settings
from functions.ssbplatforms import is_data_admin


# %%
def main() -> None:
    """Run all files."""
    is_local_files = settings.env_for_dynaconf == "local_files"
    is_admin = is_data_admin()

    if is_admin or is_local_files:
        a_collect_data.run_all()

    # No access to further processing from the kildeproject
    if is_admin:
        return

    # Only run kildomat if running locally
    if is_local_files:
        b_kildomat_local.run_all()

    # Step C: Main production part, run from the standard project
    c_pre_inndata_to_inndata.run_all()
    d_prepare_edit.run_all()


# %%
if __name__ == "__main__":
    root_logger = StatLogger()
    # Don't print debug logs from these third-party libraries
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    main()
