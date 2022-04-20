# Для запуска необходимо выполнить следующие команды
1. установить Docker
2. ```console
        $ cd /docker/docker-airflow
     ```
3. ```console
        $ docker build --rm --force-rm -t docker-airflow-spark:1.1 .
     ```
4. ```console
        $ cd /docker/docker-spark
     ```
5. ```console
        $ docker build --rm --force-rm -t docker-spark:1.1 .
     ```
6. из папки docker выполнить
    ```console
        $ docker-compose up
     ```