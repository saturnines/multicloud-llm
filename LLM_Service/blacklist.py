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