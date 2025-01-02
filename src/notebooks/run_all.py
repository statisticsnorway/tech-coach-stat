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
import a_collect_data
import b_kildomat_local
import c_pre_inndata_to_inndata


# %%
def run_all() -> None:
    """Run all files."""
    a_collect_data.run_all()
    b_kildomat_local.run_all()
    c_pre_inndata_to_inndata.run_all()


# %%
if __name__ == "__main__":
    run_all()
