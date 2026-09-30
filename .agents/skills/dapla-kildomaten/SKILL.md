---
name: dapla-kildomaten
description: Skrive, teste og rulle ut Kildomaten-skript som automatiserer overgangen fra kildedata til inndata på Dapla. Brukes når du skal lage eller endre process_source_data.py, config.yaml i et IaC-repo, feilsøke en kilde, eller trigge reprosessering av filer.
---

# Kildomaten

Kildomaten kjører et Python-skript automatisk på hver nye fil som dukker opp i teamets
kildedatabøtte, og skriver resultatet til produktbøtta. Formålet er å minimere behovet for
tilgang til kildedata: statistikkproduksjonen kan starte i en tilstand der dataminimering og
pseudonymisering allerede er gjennomført.

Dokumentasjon: https://manual.dapla.ssb.no/statistikkere/kildomaten.html

## 1. Harde krav til skriptet

1. Filen **må** hete `process_source_data.py`.
2. Koden **må** ligge i en funksjon som heter `main(source_file)`.
3. **Aldri** bruk `sys.exit()` eller tilsvarende. Det stopper hele Kildomaten og kan gi
   uprosesserte filer og uendelige omstarter. Bruk `raise` med en exception i stedet.
   Dette valideres i pull requesten og blokkerer utrulling.
4. **Bøttemontering gjelder ikke.** Bruk `gs://`-stier og `gcsfs` — ikke `/buckets/`.
5. Utfilen **må** få en sti som er unik per kildefil, ellers overskriver senere kjøringer
   tidligere resultater. Tryggeste løsning: utled målstien fra `source_file`, slik at
   strukturen i kildebøtta gjenbrukes i produktbøtta.
6. Kun et fast sett pakker er tilgjengelig i Kildomaten. Settet er definert i repoet
   `statisticsnorway/dapla-automation-processor` (`source_data/pyproject.toml`), som er lenket
   fra [manualkapittelet](https://manual.dapla.ssb.no/statistikkere/kildomaten.html). Trenger du
   flere pakker, opprett en Kundeservice-sak.

Skriptet kjøres på **én fil om gangen**.

## 2. Mal

```python
import pandas as pd

KILDE_BOETTE = "gs://ssb-dapla-example-data-kilde-prod"
PRODUKT_BOETTE = "gs://ssb-dapla-example-data-produkt-prod"
KORTNAVN = "ledstill"


def utled_periode(source_file: str) -> str:
    """Utleder perioden på navnestandardens format, f.eks. "2024-Q1".

    Args:
        source_file: Full gs://-sti til kildefilen.

    Returns:
        Perioden uten `p`-prefiks.
    """
    raise NotImplementedError("Implementeres per kilde")


def main(source_file: str) -> None:
    """Prosesserer én kildefil og skriver resultatet til produktbøtta.

    Args:
        source_file: Full gs://-sti til filen som skal prosesseres.

    Raises:
        ValueError: Hvis filen ikke ligger i den forventede kildebøtta.
    """
    if not source_file.startswith(f"{KILDE_BOETTE}/"):
        raise ValueError(f"Uventet kildesti: {source_file}")

    df = pd.read_csv(source_file)

    # Dataminimering: behold kun det som trengs for statistikken
    df = df[["col1", "col2", "col3"]]

    target = (
        f"{PRODUKT_BOETTE}/{KORTNAVN}/inndata/"
        f"skjema_p{utled_periode(source_file)}_v1.parquet"
    )
    df.to_parquet(target)
```

Kildomaten skriver **inndata**, og inndata er omfattet av navnestandarden:

- Målstien må ligge under den obligatoriske mappestrukturen `<kortnavn>/inndata/`, og filnavnet
  må ha `_p<periode>_v<versjon>`. Kildedata er unntatt navnestandarden — derfor kan du ikke
  bare speile strukturen fra kildebøtta. Se skillen `dapla-datalagring`.
- Ikke utled målbøtta med `source_file.replace("kilde", "produkt")`. `str.replace` bytter ut
  *alle* forekomster, også i mappe- og filnavn. Bygg stien eksplisitt.
- Målstien må fortsatt være unik per kildefil (krav 5 over). Hvis flere kildefiler kan havne på
  samme periode, må du enten skille dem i beskrivelsen eller slå dem sammen bevisst.

Anbefalt prosessering i Kildomaten:

- **Dataminimering** — fjern alle felt som ikke er strengt nødvendige.
- **Pseudonymisering** — av personidentifiserende data. Se skillen `dapla-metadata`.

## 3. Konfigurasjon

`config.yaml` ved siden av skriptet:

```yaml
folder_prefix: altinn   # sti i kildebøtta som trigger kjøring. "" = alle filer
memory_size: 1          # GiB, standard 512 MB, maks 32 GB. CPU settes implisitt
```

## 4. Plassering i IaC-repoet

Skript og konfigurasjon ligger i teamets IaC-repo (`<teamnavn>-iac` under `statisticsnorway`),
med **nøyaktig ett nivå** under miljømappen:

```
dapla-example-iac
└── automation/
    └── source-data/
        ├── dapla-example-prod/
        │   ├── altinn/
        │   │   ├── config.yaml
        │   │   └── process_source_data.py
        │   └── ledstill/
        │       ├── config.yaml
        │       └── process_source_data.py
        └── dapla-example-test/
            └── altinn/
                ├── config.yaml
                └── process_source_data.py
```

- Undermapper under en kilde er ikke tillatt.
- Du må alltid opprette en kildemappe, selv om du bare har én kilde.
- Kildemappens navn kan maks være 23 tegn. Dette er en teknisk begrensning i Kildomaten og
  gjelder kun denne mappen — ikke filnavn eller statistikkens kortnavn.
- Test nye kilder i `-test`-miljøet før `-prod`.

## 5. Teste lokalt

Kall `main()` nederst i skriptet fra en Dapla Lab-tjeneste med `data-admins` valgt:

```python
main("gs://ssb-dapla-example-data-kilde-prod/altinn/test20231010.csv")
```

Kjøringen vil feile når den skriver til produktbøtta, siden en `data-admins`-tjeneste ikke har
tilgang dit. Det er forventet. **Husk å fjerne kallet før utrulling.**

Pseudonymisering kan ikke testes fra en IDE i prod-miljøet. Bruk testdata i testmiljøet.

## 6. Utrulling

1. `git switch -c <gren>` i IaC-repoet, gjør endringene, push og opprett pull request.
2. PR må godkjennes av en `data-admins` på teamet.
3. Sjekk at alle tester, planer og utrullinger er grønne.
4. Merge til `main`, og følg utrullingen under Actions-fanen.

Endring av `process_source_data.py` krever ikke ny utrulling av tjenesten.
Endring av `config.yaml` gjør det.

Feiler Atlantis, skriv `atlantis plan` som kommentar i pull requesten.

## 7. Feilhåndtering og varsling

```python
# Kritisk feil: prosesseringen stopper, filen markeres som failed, e-postvarsel sendes
def main(source_file):
    raise ValueError("Kunne ikke prosessere fil fordi ...")

# Ikke-kritisk feil: prosesseringen fortsetter, filen markeres som prosessert
import logging

def main(source_file):
    logging.error("Ikke-kritisk feil")
```

Bruk exceptions for feil som gjør at prosesseringen ikke kan fortsette, og logging for feil du
vil varsles om uten å stoppe.

## 8. Monitorering

Logger finnes i Google Cloud Console → Cloud Run → Worker Pools →
`source-<kildenavn>-processor` → «View in Logs Explorer».

Sjekk status via blob-metadata (`done`, `processing`, `failed`):

```python
from google.cloud import storage

client = storage.Client()
bucket = client.bucket("ssb-dapla-example-data-kilde-prod")
for blob in bucket.list_blobs(prefix="altinn"):
    if blob.metadata.get("source-altinn-status") == "failed":
        print(blob.name)
```

Kildomaten prøver en feilet fil inntil 10 ganger før den gir opp.

## 9. Trigge reprosessering manuelt

Kun `data-admins`, fra Dapla Lab, fra et annet repo enn IaC-repoet:

```python
from dapla_toolbelt_automation import trigger_source_data_processing

trigger_source_data_processing(
    project_id="dapla-example-p",   # standardprosjektet, ikke kildeprosjektet
    source_name="altinn",           # mappenavnet i IaC-repoet
    folder_prefix="altinn/ra0678",  # mappe, eller full filsti for én enkelt fil
)
```
