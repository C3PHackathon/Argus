from invoke import task


@task
def key(c):
    """Write your TRANSIT_API_KEY to file."""
    TRANSIT_API_KEY = input("Enter the Winnipeg Transit API key: ")
    with open(".env", "w") as f:
        f.write(f"API_KEY={TRANSIT_API_KEY}")
    print('Written API_KEY to ".env"')


@task
def venv(c):
    """Create a Python3.9 virtual environment and install dependencies."""
    c.run("virtualenv venv --python /usr/local/bin/python3.9 ", pty=True)
    c.run("venv/bin/python -m pip install -r requirements.txt", pty=True)
    print("A virtual environment with Python3.9 is setup in venv")
    print("Source into it and run `jupyter notebook` in the notebooks directory.")
