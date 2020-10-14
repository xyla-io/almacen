# Almacén
Scheduling the fetching and processing of raw reports, and archiving to the data warehouse.

## Install PostgreSQL

### Ubuntu

```bash
apt update
apt install postgresql
apt install postgresql-server-dev-all
```

### OS X

```bash
brew install postgresql
```

## Install Python Virtual Environment

### Ubuntu

```bash
apt update
apt install software-properties-common
add-apt-repository ppa:deadsnakes/ppa
apt install python3.7
apt install python3.7-venv
apt install python3.7-dev
```

- [Install Python 3.7 on Ubuntu 18.04](https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/)

### OS X

```bash
brew install python3
python3 -m venv venv
```

## Install R

Install `R` as `root` for statistical tools.

### Ubuntu

```bash
echo "deb http://cran.rstudio.com/bin/linux/ubuntu trusty/" >> /etc/apt/sources.list
gpg --keyserver keyserver.ubuntu.com --recv-key E084DAB9
gpg -a --export E084DAB9 | sudo apt-key add -
apt-get update
apt-get install r-base
# verify that R was installed
R --version
# install system dependencies for the R devtools packages
apt-get install build-essential libcurl4-gnutls-dev libxml2-dev libssl-dev
```

### OS X

```bash
brew install r
brew install libgit2
```

### Install required R packages

Install R packages as the user hosting Almacén.

```R
# In R

# CRAN packages
install.packages("devtools")

# devtools packages
library(devtools)
devtools::install_github('adjust/api-client-r')
```

- [R](https://www.r-project.org/)
- [R ubuntu](https://www.digitalocean.com/community/tutorials/how-to-set-up-r-on-ubuntu-14-04)
- [devtools](https://www.digitalocean.com/community/tutorials/how-to-install-r-packages-using-devtools-on-ubuntu-16-04)
- [Adjust R Client](https://github.com/adjust/api-client-r)

## Run the install script

```bash
./install.sh
```

## Set up database environment

```bash
source venv/bin/activate
python prepare.py -t all -s <SCHEMA>
deactivate
```

## Run tests

Tests should be run with the command

```bash
python -m pytest [--pdb] [-s] [-k TEST_NAME] [TEST_PATH]
```

Running tests with the `pytest` command will result in import errors, which it may be possible to fix.

- [Pytest and sys.path/PYTHONPATH](http://doc.pytest.org/en/latest/pythonpath.html#pythonpath)