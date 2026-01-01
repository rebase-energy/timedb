import modal

# Create a Modal app
app = modal.App("timedb-api")

# Define the image with dependencies
# Using latest stable versions to avoid recursion issues
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi==0.115.6",
        "uvicorn[standard]==0.32.1",
        "click>=8.0",
        "psycopg[binary]>=3.1",
        "python-dotenv>=0.21",
        "pydantic==2.10.3",
        "pandas>=2.0.0",
        "sqlalchemy>=2.0.0",
        "pint>=0.23",
        "pint-pandas>=0.3",
    )
    .env({"PYTHONPATH": "/root"})
    .add_local_dir(
        local_path="./timedb",
        remote_path="/root/timedb",
        copy=True  # Copy files into image to ensure latest code is used
    )
)


@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("timedb-secrets")  # For DATABASE_URL/TIMEDB_DSN
    ],
    max_containers=100,  # Allow up to 100 concurrent requests
)
@modal.asgi_app()
def fastapi_app():
    # Import the FastAPI app from timedb
    import timedb.api
    return timedb.api.app