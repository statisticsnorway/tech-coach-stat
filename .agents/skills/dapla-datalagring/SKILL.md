---
name: dapla-datalagring
description: Filstier, filnavn, datatilstander, versjonering og lesing/skriving av data på Dapla. Brukes når du skal lage eller tolke en filsti i en bøtte, navngi et datasett, lagre en ny versjon, eller lese/skrive parquet, csv, xlsx eller SAS-filer på Dapla.
---

# Datalagring på Dapla

Alle data i de permanente datatilstandene lagres i Google Cloud Storage (GCS) og skal følge
[navnestandarden](https://manual.dapla.ssb.no/statistikkere/navnestandard.html).

## 1. Datatilstander

| Tilstand | Mappenavn | Innhold | Obligatorisk |
| --- | --- | --- | --- |
| Kildedata | — (egen bøtte) | Data slik de ble levert til SSB | Ja |
| Inndata | `inndata` | Kildedata konvertert til SSBs standardformat (parquet, UTF-8) | Nei |
| Klargjorte data | `klargjorte-data` | Beregnet, editert, imputert, koblet. Som regel enkeltobservasjoner | Ja |
| Statistikk | `statistikk` | Aggregerte/estimerte størrelser. Kan være konfidensielle | Nei¹ |
| Utdata | `utdata` | Statistikk der konfidensialitet er ivaretatt. Dette publiseres | Ja |

¹ Statistikk er ikke obligatorisk dersom den ikke skiller seg fra utdata.

## 2. Bøtter

For et team med teknisk navn `<team>`:

| Bøtte | Innhold | Tilgang |
| --- | --- | --- |
| `ssb-<team>-data-kilde-prod` | Kildedata | `data-admins` |
| `ssb-<team>-data-produkt-prod` | Inndata → utdata | `developers` |
| `ssb-<team>-data-kilde-test` | Kildedata, testmiljø | `data-admins` |
| `ssb-<team>-data-produkt-test` | Testdata | `developers` |

Kildedata er klassifisert som sensitive og ligger i et eget Google-prosjekt. Kode som leser
kildedata må kjøres fra en Dapla Lab-tjeneste der brukeren representerer `data-admins`.

## 3. Mappestruktur

De to første nivåene i produktbøtta er obligatoriske: **kortnavn**, så **datatilstand**.

```
ssb-dapla-example-data-produkt-prod/
└── ledstill/                     # statistikkproduktets kortnavn
    ├── inndata/
    ├── klargjorte-data/
    ├── statistikk/
    └── utdata/
```

Teamet kan lage egne undermapper under hver datatilstand. I tillegg er `temp/` og `oppdrag/`
tillatt på første nivå rett under bøttenavnet.

## 4. Filnavn

```
<kort-beskrivelse>_p<fra-periode>[_p<til-periode>]_v<versjon>.<filtype>
```

- Elementer skilles med understrek.
- Periode prefikses alltid med `p`, versjon alltid med `v`.
- Tillatte tegn: `a-z`, `A-Z`, `0-9`, `-`, `_`. **Ingen mellomrom, ingen æ/ø/å**
  (bruk `ae`, `oe`, `aa`).
- Bruk bindestrek i beskrivelser med flere ord: `grensehandel-imputert`.

### Periodeformater

| Tidsspenn | Eksempel |
| --- | --- |
| Én årgang | `flygende-objekter_p2019_v1.parquet` |
| To årganger | `ufo-observasjoner_p2019_p2020_v1.parquet` |
| Datointervall | `sykepenger_p2022-01-01_p2022-12-31_v1.parquet` |
| Tverrsnitt (status per dato) | `utdanningsnivaa_p2022-10-01_v1.parquet` |
| Måned | `grensehandel_p2022-10_v1.parquet` |
| Uke | `omsetning_p2020-W01_v1.parquet` |
| Kvartal | `pensjon_p2018-Q1_v1.parquet` |
| Tertial | `nybilreg_p2022-T1_v1.parquet` |
| Bimester | `skipsanloep_p2022-B1_v1.parquet` |
| Halvår | `personinntekt_p2022-H1_v1.parquet` |
| Dato og tid | `skjema_p2024-12-31T23-59-30.000_v1.parquet` |

### Partisjonerte data

Ved partisjonering faller filtypen bort fra navnet, og datasettnavnet blir en mappe:

```
inndata/
└── skjema_p2018_p2020_v1/
    ├── aar=2018/data.parquet
    ├── aar=2019/data.parquet
    └── aar=2020/data.parquet
```

## 5. Versjonering

Versjonering er obligatorisk. Den dekker kravet om uforanderlighet og etterprøvbarhet.

**Et datasett som er brukt i statistikkproduksjon skal aldri slettes eller overskrives.**
Opprett i stedet en ny versjon. Alle gamle versjoner skal bli liggende.

Ny versjon skal opprettes ved:

- reberegning med nye metoder
- korrigering av verdier
- observasjoner som legges til eller fjernes
- oppdatert eller erstattet kodeverk
- variabler som fjernes eller legges til
- andre strukturendringer (datatyper, formater)

Kort sagt: enhver endring skaper en ny versjon. Temporære data er unntatt.

Bruk `v0` kun midlertidig, for data som deles før de har oppnådd stabil tilstand.
Unngå uversjonerte filnavn — de gjør reproduserbarhet og dokumentasjon vanskeligere.

### Hjelpefunksjoner

Bruk [`ssb-fagfunksjoner`](https://pypi.org/project/ssb-fagfunksjoner/) framfor å skrive egen
versjonslogikk:

```python
from fagfunksjoner import (
    next_version_path,      # neste ledige versjonssti
    latest_version_path,    # sti til nyeste versjon
    latest_version_number,  # nyeste versjonsnummer
    get_fileversions,       # alle versjoner som matcher et mønster
)

ny_sti = next_version_path(
    "gs://ssb-dapla-example-data-produkt-prod/ledstill/klargjorte-data/editert_p2024-Q1_v1.parquet"
)
# -> ..._v2.parquet
```

## 6. Lese og skrive data

### På Dapla Lab (FUSE-montert)

Bøttene er montert under `/buckets/` med korte alias: `/buckets/produkt` er produktbøtta til
teamet du representerer, `/buckets/kilde` er kildebøtta. Kjør `ls /buckets` for å se hva som
faktisk er montert i tjenesten — det avhenger av hvilket team og hvilken tilgangsgruppe som ble
valgt i tjenestekonfigurasjonen. Full `gs://`-sti er ikke nødvendig.

Bruk vanlig `pandas` / `arrow` — `dapla-toolbelt` og `fellesR` er ikke nødvendig for vanlig I/O.

```python
import pandas as pd

sti = "/buckets/produkt/ledstill/klargjorte-data/editert_p2024-Q1_v1.parquet"
df = pd.read_parquet(sti)
df.to_parquet(sti)
```

Tekst, Excel og SAS leses tilsvarende med `pd.read_csv`, `pd.read_excel`,
`pd.read_sas(sti, format="sas7bdat", encoding="latin1")`.

Filoperasjoner gjøres med standardbiblioteket: `os.remove`, `shutil.copy`, `shutil.move`,
`os.mkdir`, `os.listdir`.

R:

```r
library(arrow)
df <- read_parquet(sti)
write_parquet(df, sti)
```

### Utenfor Dapla Lab (Kildomaten, automatiserte jobber)

Bøttemontering gjelder ikke. Bruk fulle `gs://`-stier og `gcsfs`:

```python
import pandas as pd
df = pd.read_parquet("gs://ssb-dapla-example-data-kilde-prod/altinn/fil.parquet")
```

### Viktig

- Parquet er standard lagringsformat. Tegnsett skal være UTF-8.
- Velg riktig Dapla-team og riktig tilgangsgruppe i tjenestekonfigurasjonen i Dapla Lab —
  det avgjør hvilke bøtter som er montert.
- Bøtter har egentlig ikke mapper; `/` er bare en del av objektnavnet. Du trenger derfor ikke
  opprette en mappe før du skriver en fil til den.
- Alt arbeid skal ligge under `$HOME/work`. Det lokale filsystemet (10 GB) er ikke et
  lagringssted for data.
