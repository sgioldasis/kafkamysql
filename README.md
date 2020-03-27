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

- [Install Docker Engine](https://docs.docker.com/install/)
- [Install Docker Compose](https://docs.docker.com/compose/install/)

## Initial Setup
It is recommended to first setup and activate a Python 3.6.8 virtualenv. If you use `pyenv` you can type the following inside your main project folder (kafkamysql):

```shell
pyenv virtualenv 3.6.8 kmtest
pyenv local kmtest
pip install --upgrade pip
```

With the above setup, next time you cd to your folder the virtualenv `kmtest` is going to be activated automatically.

After you activate your virtualenv, the next step is to install the Python requirements. To do that you can type the following inside your main project folder:

```shell
make install
```

Next, you need to create and fill in a configuration file containing Kafka and MySQL details for production. A template for this configuration file is provided. You first need to copy the template. Type the following inside your main project folder:

```shell
make config
```

Then, you can use your favorite editor to edit the `kafkamysql/config.prod.yml` file. You need to replace `<YOUR-MYSQL-HOST>` , `<YOUR-MYSQL-PASSWORD>` and `<YOUR-KAFKA-URL>` by the appropriate values for your system.

At this point, you can initialize the database (create tables, stored procedure etc.) by typing the following inside your main project folder:

```shell
make init-db
```

You can also type the above command whenever you want to re-initialize the database in the future. Note that in this case any existing data will be dropped.

## Testing

### Using Python Interpreter

You can run the tests using your local Python interpreter by typing:

```shell
make test
```

The above command will first use Docker Compose to start a local infrastructure (Zookeeper, Kafka, MySQL), then run the tests using your local Python interpreter and finally stop the local docker infrastructure. You should see in your terminal the test output and also a coverage summary. After you run the test you can also open `htmlcov/index.html` to see a detailed coverage html report in your browser.

### Using Docker

You can run also the tests using Docker by typing:

```shell
make docker-test
```

The above command will first use Docker Compose to start a dockerized version of the application and infrastructure (Zookeeper, Kafka, MySQL), then run the tests inside the application container and finally stop all the containers. You should see in your terminal all output from all the containers including the test output and a coverage summary.

### Cleaning test files

You can clean _Pytest_ and coverage cache/files by typing the following:

```shell
make clean
```

## Running

### Using Python Interpreter

You can run the program by typing:

```shell
make run
```

You will see program output in your console. Also, you might see some files under `logs` folder. File `rejected.txt` will log rejected messages (if any) and file `warnings.txt` will log MySQL warnings (if any). The program will keep running until you press `Ctrl-C` in which case it will exit.


## Blog Posts - More Information About This Repo

You can find more information about the template used to setup this project/repository and how to use it in following blog posts:

- [Ultimate Setup for Your Next Python Project](https://towardsdatascience.com/ultimate-setup-for-your-next-python-project-179bda8a7c2c)
- [Automating Every Aspect of Your Python Project](https://towardsdatascience.com/automating-every-aspect-of-your-python-project-6517336af9da)
