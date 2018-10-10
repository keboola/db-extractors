# Oracle DB Extractor

### Development ###

- clone this repository

- Build it `docker-compose build`

- To run locally make sure that you have a valid json configuration (config.json) in `path/to/my/test/data/dir` in the local repository root. Then run `docker-compose run --rm dev php /code/src/run.php --data /code/path/to/my/test/data/dir`

- To run tests `docker-compose run --rm dev`
