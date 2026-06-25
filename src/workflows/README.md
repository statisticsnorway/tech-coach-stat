# Uttesting av managed airflow

Hver folder under `workflows` svarer til en airflow, og inneholder typisk en Python-fil med selve airflow-definisjonen, og en `upload.py` som laster opp airflowen til Google.

## Eksempler

Interessante eksemplene er:
* `test_deps` som viser hvordan man gjøre steg med `task.virtualenv`, der man bruker `requirements.txt` med pinnet versjon av avhengigheter, som lastes før referansen til selve repoet, for å sikre at det er de pinnede versjonene som installeres. Å kjøre koden på denne måten krever at man bruker branchen `package-notebooks` i git-referansen, som er oppdatert slik at notebooks oppfører seg som en pakke. Om man vil teste uten `requirements.txt` kan `requirements= load_requirements()` erstattes med `requirements=["tech-coach-stat @ git+https://github.com/statisticsnorway/tech-coach-stat.git@package-notebooks"]`
* `kubeoperator` som viser et enkelt eksempel på hvordan man kan kjøre en kommando i en container

## Laste opp flyt til managed airflow

Kjør upload-scriptet for å laste opp flow til managed airflow:
```
poetry run python src/workflows/<workflow-folder>/upload.py 
```
