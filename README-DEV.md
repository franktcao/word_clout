# Setting Up Environment
This repo uses `kedro-docker` to set up your environment.
## First Time Ever?
Run, 
```bash
kedro docker init --with-spark --base-image="python:3.8-buster"  
```

## Otherwise
Run,
```bash
kedro docker build
```

# Updating Requirements
The easiest way to add requirements is to be extremely loose and just add the 
requirements without the version to the bottom of `src/requirements.txt`. Then build the 
environment with the command above (`kedro docker build`). Finally, run

```bash
docker run --rm -it word_clout:latest pip freeze > src/requirements.txt
```

# Running `kedro` Pipeline

Run
```bash
kedro docker run
```

# Clean Up Unused Docker Images

Run

```bash
docker system prune
```
