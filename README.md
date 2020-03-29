# COVID-19 Data Factory with Cloud Storage Support

COVID-19 Data Factory is a Python script that fetches actual COVID-19 data from CSSE at JHU, Worldometers and uploads it into Cloud.
The lib is ready to use as a Google Cloud Function or Amazon Lambda.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install required packages.

```bash
pip install -r requirements.txt
```

## Usage

Copy **main.py** file content into Google Cloud Function editor or Amazon Lambda and execute function.

```python

update_covid19_data()

```

To run the app on your Linux container simply run
```python

python main.py

```

You may also setup a scheduler (cron) to run the command periodically.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)