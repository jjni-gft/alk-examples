# APACHE BEAM

Przykłady bazują na przykładach znajdujących się w repozytorium https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples - serdecznie zachęcam, żeby pobawić się nimi również.

## Zawartość
Znajdziecie tu 4 przykłady, które omawialiśmy sobie na zajęciach przy okazji poznawania Apache Beam:
* `example_word_count_original` - pokazuje jak można zrobić filtrowanie i wrzucić dane do BQ
* `example_csv` - przykład pobierania danych z wielu źródeł, side inputs oraz outputs
* `example_sql` - przykład BeamSQL
* `example_flex_template` - pliki niezbędne do utworzenia flex template

Oraz kilka przykładowych plików CSV wygenerowanych przy użyciu https://extendsclass.com/csv-generator.html

## Polecenia
### Prerequisites
* musicie mieć przygotowane "virtual env" dla Python z zainstalowanym Beam'em. Dodałem przykładowy `requirements.txt` do repozytorium. Można go odpalic poleceniem `pip install -f requirments.txt` (musicie podać ścieżke do pliku, jeśli nie jesteście w tym samym folderze co ten plik).

### Odpalenie _example_csv_
Polecenie do odpalenia procesu przetwarzania (dla przykładu zadziała z folderu `beam`): 
```
python -m example_csv.transform --input gs://<bucket>/<sciezka_do_pliku> --input2 gs://<bucket>/<sciezka_do_pliku2> --output gs://<bucket>/<folder_dla_output> --temp_location gs://<bucket>/<folder_temp> --runner=DirectRunner
```
### Odpalenie _example_word_count_original_
Polecenie do odpalenia procesu przetwarzania (dla przykładu zadziała z folderu `beam`):
```
python -m example_word_count_original.wordcount --input gs://<bucket>/<sciezka_do_pliku> --output gs://<bucket>/<folder_dla_output> --output_bq=<sciezka_do_tabeli> --runner=DirectRunner
```
### Odpalenie _example_flex_template_
Polecenia niezbędne do stworzenia Flex Template:
1. Zdefiniowanie zmiennych (trzeba podstawić _project_id_, region możecie też zmienić):
```
export TEMPLATE_IMAGE="gcr.io/<project_id>/dataflow/wordcounter:latest
export REGION="europe-west3"
```
2. Zbudowanie Docker image (musicie być wewnątrz katalogu z _Dockerfile_)
```
gcloud builds submit --tag $TEMPLATE_IMAGE .
```
3. Zbudowanie template (pod podaną ścieżką zostanie zapisany deskryptor template'u)
```
gcloud dataflow flex-template build gs://<bucket>/<scieżka_do_deskryptora_szbalonu>/word_count.json --image "$TEMPLATE_IMAGE" --sdk-language "PYTHON" --metadata-file "metadata.json"
```
4. Odpalenie szablonu:
```
gcloud dataflow flex-template run "word-counter`date +%Y%m%d-%H%M%S`" --template-file-gcs-location "gs://<bucket>/<scieżka_do_deskryptora_szbalonu>/word_count.json"  --parameters input="gs://<bucket>/<scieżka_do_input>" --parameters output="gs://<bucket>/<scieżka_do_output>" --region "$REGION"
```
### Odpalenie _example_sql_
Polecenie do odpalenia procesu przetwarzania (dla przykładu zadziała z folderu `beam`):
```
python -m example_sql.wordcount_xlang_sql --input gs://<bucket>/<scieżka_do_pliku> --output gs://<bucket>/<scieżka_do_folderu_na_wyniki> --runner=DirectRunner --temp_location=gs://<bucket>/<folder_temp>
```

Gdzie:
* `runner` - w naszym przypadku będziemy używać dwóch DirectRunner (odpalenie lokalnie) oraz DataflowRunner (odpalenie na Dataflow)
* _folder_temp_ - nie będzie potrzebny, jeśli nie używacie DataflowRunner lub takich BeamIO jak dla BigQuery
* _sciezka_do_tabeli_ - scieżka w formacie project_id:dataset.nazwa_tabeli