# Lens Search backend

This repository holds the backend source code for  Lens Search. To run this repository you will need Python 3.7+ installed. For the virtual environment you could use Pipenv

1. Activate the virtual environment
```
pipenv shell
```

2. Istall the dependecies
```
pipenv install
```

3. Set the depencies and required environment variables

* Elasticsearch configurations - search
* Redis configurations - cache
* ArangoDB - graph


4. Run the application 

```
python run.py
```

## Lens backend API

API:

https://lens-api.sepana.io/

Swagger Doc:

https://lens-api.sepana.io/docs