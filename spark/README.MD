# APACHE SPARK

Przykłady bazują na przykładach znajdujących się w repozytorium https://github.com/apache/spark/tree/master/examples/src/main/python - serdecznie zachęcam, żeby pobawić się nimi również.

## Zawartość
Znajdziecie tu 5 przykładów, które omawialiśmy sobie na zajęciach przy okazji poznawania Apache Beam:
* `example_wordcount_rdd` - pokazuje jak można zrobić filtrowanie, mapowanie korzystając z nispoziomowego API RDD
* `example_wordcount` - pokazuje jak można zrobić filtrowanie, mapowanie korzystając z wysokopoziomowego API DataFrame
* `example_csv` - przykład pobierania danych z plików CSV
* `example_sql.wordcount.py` - przykład wykorzystania SparkSQL
* `example_sql.wordcount_hive.py` - przykład wykorzystania SparkSQL/HiveQL oraz Dataproc Metastore

Oraz definicje workflow pliku `workflow.yaml`

## Polecenia
### Prerequisites
* Do ręcznego uruchamiania lokalnie musicie mieć przygotowane "virtual env" dla Python z zainstalowanym pySpark'iem. Dodałem przykładowy `requirements.txt` do repozytorium. Można go odpalic poleceniem `pip install -f requirments.txt` (musicie podać ścieżke do pliku, jeśli nie jesteście w tym samym folderze co ten plik).

### Odpalenie jobów
* Podczas zajęć ja odpalałem wszystkie joby poprzez skopiowanie pliku na GCS i wyklikanie job'a z UI - co jest chyba najprostszą metodą. Jeśli ktoś chciałby odpalić proces z linii poleceń to można skorzystać z następujących poleceń.

```
gsutil cp <plik_ktory_chcecie_testowac> gs://<bucket>/<folder: opcjonalnie>
gcloud dataproc jobs submit pyspark \
    --cluster=<nazwa klastra> \
    --region=<region> \
    <plik python do wykonania - np. wordcount.py> \
    -- gs://<bucket>/<sciezka_do_pliku> gs://<bucket>/<folder_dla_output>
```
