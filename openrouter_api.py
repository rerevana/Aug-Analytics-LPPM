import requests
from config import API_KEY, PROJECT_TITLE, REFERER # Mengambil konfigurasi dari config.py

def call_openrouter(messages, max_tokens=3000, model="meta-llama/llama-3.3-70b-instruct", temperature=0.3):
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": REFERER,
        "X-Title": PROJECT_TITLE
    }

    data = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens
    }

    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    result = response.json()
    return result["choices"][0]["message"]["content"].strip()
