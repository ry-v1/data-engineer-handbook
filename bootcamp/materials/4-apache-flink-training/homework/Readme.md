# Instructions

- It includes a Flink job that sessionizes the input data by IP address and host which use a 5 minute gap


## :boom: Running the pipeline

1. Build the Docker image and deploy the services in the `docker-compose.yml` file, including the PostgreSQL database and Flink cluster. This will (should) also create the sink table, `processed_events`, where Flink will write the Kafka messages to.

    ```bash
    make up

    #// if you dont have make, you can run:
    # docker compose --env-file flink-env.env up --build --remove-orphans  -d
    ```

    **:star: Wait until the Flink UI is running at [http://localhost:8081/](http://localhost:8081/) before proceeding to the next step.** _Note the first time you build the Docker image it can take anywhere from 5 to 30 minutes. Future builds should only take a few second, assuming you haven't deleted the image since._

    :information_source: After the image is built, Docker will automatically start up the job manager and task manager services. This will take a minute or so. Check the container logs in Docker desktop and when you see the line below, you know you're good to move onto the next step.

    ```
    taskmanager Successful registration at resource manager akka.tcp://flink@jobmanager:6123/user/rpc/resourcemanager_* under registration id <id_number>
    ```
   2. Make sure to run `sql/init.sql` on the postgres database from Week 1 and 2 to have the `processed_events` table appear
3. Now that the Flink cluster is up and running, it's time to finally run the PyFlink job! :smile:

    ```bash
    make job

    #// if you dont have make, you can run:
    # docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py -d
    ```

    After about a minute, you should see a prompt that the job's been submitted (e.g., `Job has been submitted with JobID <job_id_number>`). Now go back to the [Flink UI](http://localhost:8081/#/job/running) to see the job running! :tada:


### **Run Postgres and PGAdmin in Docker**

- Install Docker Desktop from **[here](https://www.docker.com/products/docker-desktop/)**.

- Copy **`example.env`** to **`.env`**:
    
    ```bash
    cp example.env .env
    ```

- Start the Docker Compose container:
        ```bash
        docker compose up -d
        ```
        
- SQL queries will provide the answers below questions.
  - What is the average number of web events of a session from a user on Tech Creator?
  - Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)