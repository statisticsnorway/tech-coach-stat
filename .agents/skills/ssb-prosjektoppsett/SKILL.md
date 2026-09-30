---
name: ssb-prosjektoppsett
description: Opprette og vedlikeholde SSB-prosjekter med ssb-project, Poetry og renv, sette opp linting, typesjekking, testing, pre-commit og CI, og følge SSBs regler for kode (KVAKK) og git-arbeidsflyt. Brukes ved nytt prosjekt, nye avhengigheter, oppsett av kodekvalitetsverktøy, eller spørsmål om hva som er lov å sjekke inn.
---

# Prosjektoppsett og kodekvalitet i SSB

## 1. ssb-project

```shell
ssb-project create <prosjektnavn>              # nytt prosjekt
ssb-project create <prosjektnavn> --github     # nytt prosjekt med GitHub-repo
ssb-project build                              # bygg miljø etter kloning
ssb-project clean <prosjektnavn>               # slett kernel og pakker
```

`ssb-project create` gir standard mappestruktur, Git med SSBs `.gitignore`/`.gitattributes`,
et Poetry-miljø og en Jupyter-kernel.

```
<prosjektnavn>/
├── src/            # all produksjonskode
│   └── notebooks/  # notebooks som orkestrerer
├── tests/          # enhetstester
├── pyproject.toml
├── poetry.lock
├── LICENSE
└── README.md
```

Etter kloning av et eksisterende prosjekt: alltid `ssb-project build` (eller `poetry install`).
Kernelen kan bruke ~30 sekunder på å dukke opp i launcheren.

Dokumentasjon: https://statisticsnorway.github.io/ssb-project-cli/

## 2. Pakkehåndtering

**Python:**

```shell
poetry add <pakke>            # aldri pip install i et ssb-project
poetry add --group dev <pakke>
poetry run python <script.py>
poetry run pytest
```

`poetry.lock` skal alltid sjekkes inn — den er det som gjør miljøet reproduserbart.

**R:** bruk `renv`. Dette er ikke integrert i `ssb-project` og må settes opp separat.

**Full disk?** Dapla Labs lokale filsystem har bare 10 GB. Slett ubrukte virtuelle miljøer
(`ssb-project clean`) og `~/.cache/`. Kode er permanent lagret i GitHub, ikke lokalt.

## 3. Kodekvalitetsverktøy

**Les prosjektets `pyproject.toml` og `.pre-commit-config.yaml` før du foreslår verktøy eller
kommandoer.** Ikke anta et fast sett — oppsettet varierer mellom prosjekter og har endret seg
over tid.

Typisk oppsett i SSB i dag:

| Formål | Verktøy |
| --- | --- |
| Linting og import-sortering | `ruff` |
| Formattering | `black` (ev. `ruff format`) |
| Typesjekking | `mypy`, gjerne `strict = true` |
| Testing | `pytest` med `pytest-cov` |
| Automatisering ved commit | `pre-commit` |
| Avhengighetssjekk | `deptry` |

Kjør verktøyene og les den faktiske feilmeldingen framfor å gjette:

```shell
poetry run ruff check .
poetry run black .
poetry run mypy src
poetry run pytest
poetry run pre-commit run --all-files
```

## 4. Kodekonvensjoner

- **Type-annotasjoner** på alle offentlige funksjoner.
- **Docstrings** på offentlige funksjoner og klasser. Google-stil er vanligst i SSB
  (`convention = "google"` i ruff/pydocstyle).
- **Logikk i funksjoner i `src/`**, notebooks orkestrerer. Da kan logikken enhetstestes.
- **Notebooks** lagres gjerne som `.py` i jupytext percent-format, slik at de kan kodeanalyseres
  og versjonshåndteres meningsfullt. Sjekk `[tool.jupytext]` i `pyproject.toml`.
- **Konfigurasjon** (filstier, perioder, teamnavn) i en konfigurasjonsfil, f.eks. Dynaconf
  `settings.toml` — ikke hardkodet i hver notebook.
- **Feilhåndtering** med exceptions. Ikke `sys.exit()` og ikke `print()` som feilmekanisme.
- **Datavalidering** av dataframes med `pandera` er god praksis.
- **Tester** for all ny logikk. Bruk syntetiske testdata, aldri skarpe data.

## 5. Regler fra KVAKK

[KVAKK](https://manual.dapla.ssb.no/statistikkere/kvakk.html) fastsetter regler som skal følges
med mindre avvik er dokumentert og begrunnet:

- All produksjonskode skal være under versjonskontroll i GitHub.
- Kildekode i GitHub skal ikke inneholde ukrypterte passord eller hemmeligheter.
- Git-klienter skal konfigureres slik at resultat fra Jupyter-notebooks ikke lagres på GitHub.
- All produksjonskode skal lagres i et format som støtter kodeanalyse.
- Biblioteker skal ha en eier, dokumentert grensesnitt og tilhørende tester.
- Bruk SSBs mal for PyPI-biblioteker når du lager et Python-bibliotek.

### Hemmeligheter

Bruk **Google Secret Manager**, eller en `.env`-fil som er dekket av `.gitignore`. Legg aldri
nøkler, tokens eller passord i kode, notebooks eller innsjekket konfigurasjon. Oppdager du en
hemmelighet i repoet, si fra til brukeren framfor å flytte den stille.

## 6. Git-arbeidsflyt

```shell
git switch main && git pull          # oppdater main
git switch -c <beskrivende-gren>     # ny gren
# gjør endringer
git add <filer> && git commit -m "<hva og hvorfor>"
git push
git fetch && git merge origin/main   # løs merge-konflikter lokalt
```

Deretter: opprett pull request på GitHub, be en kollega om review, merge og slett grenen.

- Push aldri direkte til `main`.
- All produksjonskode skal ligge under `statisticsnorway`-organisasjonen på GitHub.
- GitHub-repoer i SSB kan ikke slettes, kun arkiveres.

## 7. CI

Bruk GitHub Actions til å kjøre tester og kodekvalitetssjekker på push og pull request.
Mange SSB-repoer sender i tillegg dekningsrapport til SonarQube Cloud
(`sonar-project.properties`).
