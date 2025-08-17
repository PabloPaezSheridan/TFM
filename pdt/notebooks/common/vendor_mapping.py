EXCEPTIONS = {
    "BRK.A": "BRK-A",
    "BRK.B": "BRK-B",
    "BF.B":  "BF-B",
}

def to_yfinance_symbol(sym: str) -> str:
    s = sym.strip().upper()
    if s in EXCEPTIONS:
        return EXCEPTIONS[s]
    if s.startswith("^"):
        return s
    return s.replace(".", "-")