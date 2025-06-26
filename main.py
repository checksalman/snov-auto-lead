from fastapi import FastAPI

app = FastAPI()

@app.get("/test-api")
async def test_api():
    return {"message": "FastAPI on Render is working!"}
