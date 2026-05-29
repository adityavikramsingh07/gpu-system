from .base_chain import invoke_gemini

def run_correlation_chain(system_prompt: str, query: str, analysis: str) -> str:
    prompt = (
        f"Original Query: {query}\n\n"
        f"Analysis Findings:\n{analysis}\n\n"
        "Correlate the findings to directly answer the user's query. Highlight risks or alignment."
    )
    return invoke_gemini(system_prompt, prompt)
