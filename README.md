# Kafka to MySQL
![Language](https://img.shields.io/badge/python-v3.6.8-blue)
![Author](https://img.shields.io/badge/Made%20By-Savas%20Gioldasis-blue)

This project is a Python implementation of a Kafka consumer that reads JSON messages from a Kafka topic, transforms them to a corresponding relational schema and inserts them into a MySQL table. It also creates a MySQL stored procedure which aggregates the ingestion table into a second (aggregate) table and schedules the stored procedure to run hourly.

## Prerequisites

Before you begin, ensure you have met the following requirements:
<!--- These are just example requirements. Add, duplicate or remove as required --->
* A `Linux` machine
* Git
* Python 3.6.8 (and above)
* Docker and docker-compose

*Note:* The following instructions are for Ubuntu Linux but should work in any Debian based Linux.

### Clone this repo

```bash
git clone git@github.com:sgioldasis/kafkamysql.git
cd kafkamysql
```

### Install Git

```bash
sudo apt update
sudo apt install git
```

### Install Python

You can follow any method appropriate for your system to install Python 3.6.8. Using a Python virtual environment is recommended. If your system already has another python version you can follow the link to install
[multiple python versions with pyenv](https://realpython.com/intro-to-pyenv/)

Once you have installed Python you also need to install the Python `pip` package manager. For example you can run the following commands:

```bash
sudo apt install python-pip
pip install --upgrade pip
```

### Install Docker Engine and Docker Compose

You can find instructions for your system in the links below:

https://docs.docker.com/install/

https://docs.docker.com/compose/install/

## Initial Setup
It is recommended to first setup and activate a Python 3.6.8 virtualenv. If you use `pyenv` you can type the following inside your main project folder (kafkamysql):

```shell
pyenv virtualenv 3.6.8 kmtest
pyenv local kmtest
```
With the above setup, next time you cd to your folder the virtualenv `kmtest` is going to be activated automatically.

After you activate your virtualenv, the next step is to install the Python requirements. To do that you can type the following inside your main project folder:

```shell
make install
```

Next, you need to create and fill in a configuration file containing Kafka and MySQL details for production. A template for this configuration file is provided. You first need to copy the template. Type the following inside your main project folder:

```shell
cp kafkamysql/config.prod.template.yml kafkamysql/config.prod.yml
```

Then, you can use your favorite editor to edit the `kafkamysql/config.prod.yml` file. You need to replace `<YOUR-MYSQL-HOST>` , `<YOUR-MYSQL-PASSWORD>` and `<YOUR-KAFKA-URL>` by the appropriate values for your system.




## Running

### Using Python Interpreter
```shell
~ $ make run
```

### Using Docker

Development image:
```console
~ $ make build-dev
~ $ docker images --filter "label=name=blueprint"
REPOSITORY                                                             TAG                 IMAGE ID            CREATED             SIZE
docker.pkg.github.com/martinheinz/python-project-blueprint/blueprint   3492a40-dirty       acf8d09acce4        28 seconds ago      967MB
~ $ docker run acf8d09acce4
Hello World...
```

Production (Distroless) image:
```console
~ $ make build-prod VERSION=0.0.5
~ $ docker images --filter "label=version=0.0.5"
REPOSITORY                                                             TAG                 IMAGE ID            CREATED             SIZE
docker.pkg.github.com/martinheinz/python-project-blueprint/blueprint   0.0.5               65e6690d9edd        5 seconds ago       86.1MB
~ $ docker run 65e6690d9edd
Hello World...
```

## Testing

Test are ran every time you build _dev_ or _prod_ image. You can also run tests using:

```console
~ $ make test
```

## Pushing to GitHub Package Registry

```console
~ $ docker login docker.pkg.github.com --username MartinHeinz
Password: ...
...
Login Succeeded
~ $ make push VERSION=0.0.5
```

## Cleaning

Clean _Pytest_ and coverage cache/files:

```console
~ $ make clean
```

Clean _Docker_ images:

```console
~ $ make docker-clean
```

## Setting Up Sonar Cloud
- Navigate to <https://sonarcloud.io/projects>
- Click _plus_ in top right corner -> analyze new project
- Setup with _other CI tool_ -> _other_ -> _Linux_
- Copy `-Dsonar.projectKey=` and `-Dsonar.organization=`
    - These 2 values go to `sonar-project.properties` file
- Click pencil at bottom of `sonar-scanner` command
- Generate token and save it
- Go to repo -> _Settings_ tab -> _Secrets_ -> _Add a new secret_
    - name: `SONAR_TOKEN`
    - value: _Previously copied token_
    
## Creating Secret Tokens
Token is needed for example for _GitHub Package Registry_. To create one:

- Go to _Settings_ tab
- Click _Secrets_
- Click _Add a new secret_
    - _Name_: _name that will be accessible in GitHub Actions as `secrets.NAME`_
    - _Value_: _value_

## Blog Posts - More Information About This Repo

You can find more information about the template used to setup this project/repository and how to use it in following blog posts:

- [Ultimate Setup for Your Next Python Project](https://towardsdatascience.com/ultimate-setup-for-your-next-python-project-179bda8a7c2c)
- [Automating Every Aspect of Your Python Project](https://towardsdatascience.com/automating-every-aspect-of-your-python-project-6517336af9da)
