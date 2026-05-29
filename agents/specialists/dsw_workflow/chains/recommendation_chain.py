from .base_chain import invoke_gemini

def run_recommendation_chain(system_prompt: str, correlation: str) -> str:
    prompt = (
        f"Correlated Context:\n{correlation}\n\n"
        "Based strictly on this context, provide 3 strategic recommendations. Format them clearly."
    )
    return invoke_gemini(system_prompt, prompt)
