# CS5052-spark

## Dependencies

Web app is bult with `dash` and `dash-bootstrap-components `.

## To run

- Ensure PySpark is installed and accessible
- Install dependencies

```
pip install dash dash_bootstrap_components pandas geojson_rewind dash-daq
```

### Run CLI
```
python p1_main.py
```


### Run web app
```
python webapp/app.py
```

### Troubleshooting
1. `webapp` module not found
Ensure `webapp` module is in Python path:
```
export PYTHONPATH=$PYTHONPATH:<root module path>
```