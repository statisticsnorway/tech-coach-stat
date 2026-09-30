---
name: dapla-metadata
description: Dokumentasjon av datasett og variabler på Dapla med Datadoc og Vardef, navngiving av variabler (kortnavn), og pseudonymisering av personopplysninger. Brukes når du skal dokumentere et datasett, velge variabelnavn, eller pseudonymisere data.
---

# Metadata og pseudonymisering

## 1. Datadoc — dokumentasjon av datasett

[Datadoc](https://manual.dapla.ssb.no/statistikkere/datadoc.html) er SSBs system for
dokumentasjon av datasett på Dapla. Metadata lagres tett knyttet til datasettet.

Dokumentasjonskravene følger datatilstanden:

| Datatilstand | Krav |
| --- | --- |
| Kildedata | Datasettnivå: dataeier, område, tidsinformasjon. Begrenset variabelinformasjon |
| Inndata | I utgangspunktet som kildedata |
| Klargjorte data | Variabeldefinisjoner og nøyaktighetsforbedrende tiltak som er utført |
| Statistikk | Variabeldefinisjoner, samt metoder og kode brukt for å produsere statistikken |
| Utdata | I utgangspunktet som statistikk |

Kravene er strengest for klargjorte data, statistikk og utdata, fordi disse skal kunne deles.

Dokumentér både **datasettet som helhet** (beskrivelse, eierteam, om det inneholder
personopplysninger, bruksrestriksjoner) og **hver kolonne** (variabelforekomst): beskrivelse
via lenke til Vardef, måleenhet, datatype, og om verdiene er pseudonymisert.

Noen felter genereres maskinelt: identifikator, filsti, og hvilke datoer datasettet dekker.
Det forutsetter at filnavnet følger navnestandarden — se skillen `dapla-datalagring`.

Verktøy:

- **Datadoc-editor** — grafisk grensesnitt i Dapla Lab
- **`dapla-toolbelt-metadata`** — programmatisk tilgang

```python
from dapla_metadata.datasets import Datadoc

# Les alltid pakkens faktiske API før du skriver kode mot den.
```

## 2. Vardef — variabeldefinisjoner

Variabler skal defineres i Vardef, og kolonner i Datadoc skal lenke til definisjonen der.

### Krav til kortnavn

- Kun `a-z` (små bokstaver), `0-9` og `_`
- Minst 2 tegn, og må starte med en bokstav
- Generiske navn (`kilde`, `total`, `kode`) må spesifiseres med suffiks: `kilde_innt`

### Anbefalinger

- Bruk prefiks for å skille variabler med samme navn, f.eks. enhetstype: `pers_innt`,
  `hush_innt`. Ved behov også statistikkens kortnavn: `selvangivelse_pers_innt`.
- Bruk suffiks for å detaljere eller aggregere: `pers_id_mor`, `pers_innt_gjsnitt`.
- Erstatt æ/ø/å med `ae`/`oe`/`aa`: `naering`, `loenn`, `aar`.
- Velg navn med semantisk likhet til det fulle variabelnavnet: `bra` (bruksareal).
- Ingen fast maksgrense for lengde — balanser beskrivende navn mot praktisk programmering.

## 3. Pseudonymisering

[Pseudonymisering](https://manual.dapla.ssb.no/statistikkere/dapla-pseudo.html) av
personidentifiserende data gjøres med `dapla-toolbelt-pseudo`.

Regler:

- Pseudonymisering bør skje så tidlig som mulig — normalt i **Kildomaten**, i overgangen
  kildedata → inndata. Da slipper resten av produksjonsløpet å håndtere klartekst.
- Pseudonymisering **kan ikke kjøres fra en IDE i prod-miljøet**. Det er et bevisst valg for
  å hindre at upseudonymiserte og pseudonymiserte data ses samtidig. Test i testmiljøet med
  testdata.
- Dokumentér i Datadoc at kolonnen er pseudonymisert.
- Skriv aldri klartekst-identifikatorer til logg, feilmeldinger eller notebook-output.

Les pakkens faktiske API før du skriver kode — ikke gjett på funksjonsnavn eller signaturer.

## 4. Sjekkliste før et datasett deles

- [ ] Filnavn følger navnestandarden, med periode og versjon
- [ ] Datasettet ligger i riktig datatilstand-mappe i produktbøtta
- [ ] Datasettet er dokumentert i Datadoc
- [ ] Alle kolonner er dokumentert og lenket til Vardef
- [ ] Personopplysninger er pseudonymisert og merket som det
- [ ] Ingen tidligere versjon er overskrevet eller slettet
