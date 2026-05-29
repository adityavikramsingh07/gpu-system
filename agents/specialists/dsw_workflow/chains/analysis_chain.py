import json
from .base_chain import invoke_gemini

def run_analysis_chain(system_prompt: str, region: str, material: str, tool_results: dict) -> str:
    prompt = (
        f"Analyze the following raw tool data for the region: {region} "
        f"concerning material: {material}.\n\n"
        f"Tool Data:\n{json.dumps(tool_results, indent=2)}\n\n"
        "Provide a factual, concise analysis extracting key metrics and findings."
    )
    return invoke_gemini(system_prompt, prompt)
