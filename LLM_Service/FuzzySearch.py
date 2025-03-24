from thefuzz import fuzz, process
from LLM_Service.DataParse import query_data
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import re
app = FastAPI()
from blacklist import BLACKLIST



# may need to change this depending on how llm is
class FuzzyResponse(BaseModel):
    match: str

@app.get("/fuzzy", response_model=FuzzyResponse)
async def fuzzy_endpoint(query: str):
    if not query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    words = re.findall(r'\b\w+\b', query.lower())
    result = fuzzy_match(words)

    if not result:
        raise HTTPException(status_code=404, detail="No match found")

    return {"match": result}

def fuzzy_match(query: list):
    """
    Match a list of words against QueryData using fuzzy matching and a sliding window.

    Returns:
        str: Best matched item name or None if no match found
    """

    #  filter blacklist stuff
    query = [word for word in query if word and word.lower() not in BLACKLIST]
    if not query:
        return None

    best_match = ""
    l = 0
    best_score = 0

    for r in range(len(query)):
        curr = query[l:r + 1]
        curr_str = " ".join(curr)

        matches = process.extract(curr_str, query_data.keys())

        best_match_current = max(matches, key=lambda x: x[1])

        if best_match_current[1] == 100:
            best_match = best_match_current[0]
            break
        elif best_match_current[1] > best_score:
            best_score = best_match_current[1]
            best_match = best_match_current[0]

        if (r - l) >= 3:
            l += 1

    return best_match




if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
