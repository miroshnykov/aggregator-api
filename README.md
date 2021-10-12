# co-aggregator

> aggreagator
## collect data


## send to s3


## Send from s3 to redshift

## Docker setup
	docker build -t co-aggragator .
	docker run -it -p 3001:3001 --rm --name co-aggragator-  co-aggragator

## run
    npm run dev

## build
    npm run build

## env example
    HOST=localhost
    PORT=9002
    NODE_ENV = development
    FOLDER_LOCAL = unprocessed_json
    #EX local dir_current = /home/miroshnykov/Sites/co-aggragator/unprocessed_json
    AWS_ACCESS_KEY_ID=
    AWS_SECRET_ACCESS_KEY=
    AWS_REGION=us-east-1

    S3_BUCKET_NAME = co-aggregator-staging

    REDSHIFT_HOST = 
    REDSHIFT_USER = 
    REDSHIFT_PORT = 5439
    REDSHIFT_PASSWORD = 
    REDSHIFT_DATABASE = 

# diagram
![](diagram-co-traffic.png)