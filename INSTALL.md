# OpenEBench VRE Nextflow Executor install instructions

## Install Nextflow and its dependencies, used by the wrapper

1. Docker must be installed and running in the machine, as it is a requisite for this code. If it is not, you only have to run next command

 ```bash
 # These are pre-requisites for docker, described at https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-repository
 sudo apt update
 sudo apt install apt-transport-https ca-certificates curl software-properties-common
 curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
 sudo add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) \
    stable"
 
 # This is the docker package as such
 sudo apt update
 sudo apt install docker-ce
 ```

 Remember to add the username to the `docker` group, using for instance next recipe.

 ```bash
 sudo usermod -a -G docker $USER
 ```

2. Java JVM 8 or later must be installed, as it is a pre-requisite for Nextflow. These are the commands to install OpenJDK in Ubuntu:

 ```bash
 sudo apt update
 sudo apt install default-jdk
 ```

3. [Nextflow](https://www.nextflow.io/) should be installed with next instructions, so the installed version is assured:

```
TODO
```

## Install the wrapper dependencies

* Python 2

```bash
virtualenv -p /usr/bin/python2 .py2Env
source .py2Env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
* Python3

```bash
python3 -m venv .py3Env
source .py3Env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

