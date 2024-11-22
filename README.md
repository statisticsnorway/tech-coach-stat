# tech-coach-stat

Example of how to produce statistics for meteorological data, for use in Statistics Norway.
The rest of this description is in Norwegian.

Dette repoet brukes til to ting:

1. Det er et eksempel på hvordan man kan lage et statistikkproduksjonsløp på Dapla.
   Målet er vise organisering, bruk av navnestandard, automatisering, kodekvalitet,
   prosessteg osv. som følger retningslinjer fra [Standardutvalget], [KVAKK] og [Dapla-manualen].
   Statistikken er en enkel fiktiv statistikk basert på åpne værdata fra
   Meteorologisk institutts [Frost API].
   Selve statistikkoden er forenklet og er der bare for å vis de andre delene.

2. Det brukes av tech-coachene på seksjon S703 IT-Partner for å teste ut nye
   anbefalinger og måter å gjøre ting på.

## Plattformer

- Koden virker på:
  - Dapla
  - DaplaLab, både med bøtter og filer, på både Jupyter og vscode.
  - Windows og Linux lokalt.

## Hva viser repoet?

Til nå viser repoet:

- Datafangst via API
- Håndtering av hemmeligheter
- Bruk av konfigurasjonsfiler ved hjelp av [Dynaconf]. Filstier, perioder osv. legges
  in konfigurasjonsfil i stedet for å hardkodes i hver enkelt notebook.
- Versjonering av filer (delvis)
- Bruk av kodekvalitetsverktøy
- Organisering i form av funksjoner og automatisert kjøring av disse.
- Enhetstesting, det vil si testing av logikken i funksjonene.
- Automatisert testing ved hjelp av GitHub Actions
- Kildomaten
- Datatilstander: Fram til pre-inndata

Det neste på blokka:

- Datatilstand: Fram til inndata.

## Konfigurasjonsfil og miljøer

I `config`-katalogen ligger det `settings.toml`-fil hvor man setter opp felles
konfigurasjon. Der definerer man blant annet navn på dapla-team, kortnavn på
statistikken, filstier, perioder osv. Den definerer tre "miljøer":

- **default:** Bruk av bøtter, enten på gamle Dapla eller på DaplaLab
- **daplalab_files:** Bruk bøtter montert som filer på DaplaLab
- **local_files:** Bruk av lokale filer under en `data`-katalog i repoet.

Valg av miljø gjør man i filen `src/functions/config.py`.

I koden brukes det gjennomgående type `str` for å angi filstier i bøtter,
og type `pathlib.Path` for å angi stier til filer på et filsystem.
Funksjonene som bruker filstier er generelle, og sjekker om filsti-typen er `str`
eller `pathlib.Path` for å bestemme hvilken implementasjon som skal velges.

## Hvordan komme i gang?

Start med å klone ut repoet og kjør kommandoen `ssb-project build` eller
`poetry install`.

### Frost Client ID

For å få tilgang til Frost API'et så trenger du en Frost client ID. Den får du ved
å registrere deg som bruker på siden https://frost.met.no/howto.html.

Kopier client ID'en du får inn en ny `.env`-fil i rot-mappen på repoet. Den vil se
ut noe sånt som dette:

```
FROST_CLIENT_ID="5dc4-mange-nummer-e71cc"
```

**Merk:** Dette må du gjøre hver gang du kloner ut repoet, siden .env-filen ikke skal
ligge i git.

### Juster konfigurasjonsfiler

Repoet er som standard satt opp til å kjøre mot bøtter i dapla teamet `tip-tutorials`.
Hvis du vil teste det hos deg, så sett `env="local_files"` i fila
`src/functions.config.py`. Da vil alle filer lagres lokalt i data-mappen i repoet.

### Kjøring

Nå er alt klart til å kjøre koden.

Foreløpig er det bare det fra datafangst og fram til pre-inndata som er på plass.

Kjør filen `src/notebooks/collect_data.py` enten ved å åpne den i vscode eller Jupyter,
og kjør den derfra. Eller fra kommandolinja:

```shell
python src/notebooks/collect_data.py
```

[Dapla-manualen]: https://manual.dapla.ssb.no/
[Dynaconf]: https://www.dynaconf.com/
[Frost API]: https://frost.met.no/index.html
[KVAKK]: https://statistics-norway.atlassian.net/wiki/spaces/BEST/pages/3261497397/Kvalitet+i+kode+og+koding
[Standardutvalget]: https://ssbno.sharepoint.com/sites/Avdelingerutvalgograd/SitePages/Vedtak-fra-Standardutvalget.aspx
