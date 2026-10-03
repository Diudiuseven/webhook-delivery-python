"""Entry point: builds the FastAPI application with the delivery layer."""
import uvicorn

from app.routes import create_app

app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
