from thefuzz import fuzz,process
from LLM_Service.DataParse import query_data
from fastapi import FastAPI

app = FastAPI()



@app.get("/ping")
async def ping():
    return {"message": "pong"}



async def fuzzy_match(query: str, choices: List[str]):



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)