# Lens Search backend

This repository holds the backend source code for Lens Search. Click here to search: https://lens.sepana.io/
Open APIs are available to provide intergrated full-text search for any Lens dapp.

- Data was called from the Lens protocol using [`The Graph`](https://thegraph.com/hosted-service/subgraph/anudit/lens-protocol?query=Get%20Posts)
- A full-text search engine was built ontop of the Lens data to provide advanced search and filtering. More on that [here](https://github.com/sepana-io/lens-search-frontend)
- Social graph visualization was built using `Lens graphql APIs` as well as `ArangoDB` graph tooling
- `SwaggerAPI` was used to create API docs and endpoints


## Lens backend API

API:

https://lens-api.sepana.io/

Swagger Doc:

https://lens-api.sepana.io/docs


## How to run the backend

To run this repository you will need Python 3.7+ installed. 

1. Activate the virtual environment
```
pipenv shell
```

2. Install the dependecies
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

For any questions or help integrating the APIs, feel free to contact daniel at sepana.io

