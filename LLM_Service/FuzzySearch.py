from thefuzz import fuzz, process
from LLM_Service.DataParse import query_data
from fastapi import FastAPI
from typing import List, Optional, Dict, Any

app = FastAPI()


@app.get("/ping")
async def ping():
    return {"message": "pong"}


def fuzzy_match(query: list):
    #  words blacklist (lots of false positives)
    BLACKLIST = {
        # verbs
        "is", "am", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "doing",
        "using", "making", "getting", "running", "going",

        # prepositions
        "the", "a", "an", "and", "but", "or", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "up", "down", "into", "onto", "upon",

        # Numbers (May need to adjust since some stuff has numbers)
        "one", "two", "three", "four", "five", "day", "week", "hour", "minute",
        "first", "second", "third", "last", "next",

        # pronouns
        "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them",
        "my", "your", "his", "her", "its", "our", "their",

        # internet common chat xd
        "rn", "ngl", "tbh", "fr", "lol", "lmao", "yeah", "nah", "Im", "im",

        # trading specific filler
        "price", "profit", "selling", "buying", "trade", "trading", "cost", "worth",
        "market", "deal", "cheap", "expensive", "method", "way", "strategy",

        # quantities
        "some", "many", "much", "few", "lot", "lots", "tons", "bit",

        # common adjectives
        "good", "bad", "best", "worst", "better", "worse", "nice", "great",
        "awesome", "cool", "real", "fake", "true", "false", "special", "secret",

        # question words
        "what", "when", "where", "who", "why", "how", "which",

        # time related
        "now", "later", "soon", "today", "tonight", "tomorrow", "yesterday",
    }

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




print(fuzzy_match(["EnChAnTeD", "DiAmOnD", "block"]))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
