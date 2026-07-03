#!/usr/bin/env python3
# ── Mosaic Setup Wizard ─────────────────────────────────────────────────────
#
# Interactive terminal script to configure the .env file on first run.

import os
import sys

def prompt_input(question, default=None, sensitive=False):
    prompt = f"{question} "
    if default is not None and not sensitive:
        prompt += f"[{default}]: "
    elif default is not None:
        prompt += "[has default]: "
    else:
        prompt += ": "
        
    try:
        val = input(prompt).strip()
    except (KeyboardInterrupt, EOFError):
        print("\nSetup cancelled.")
        sys.exit(1)
        
    if not val:
        return default
    return val

def run_wizard():
    print("=================================================================")
    print(" 🪙  Mosaic Configuration Wizard")
    print("=================================================================")
    print(" This wizard will guide you through setting up your .env file.")
    print(" Press Enter to accept the defaults (shown in brackets).\n")

    if os.path.exists(".env"):
        overwrite = prompt_input("A '.env' file already exists. Overwrite it? (y/n)", default="n").lower()
        if overwrite != "y":
            print("Setup aborted. Your existing '.env' file was preserved.")
            sys.exit(0)

    # 1. Choose LLM Provider
    print("--- 1. LLM / AI Configuration ---")
    print("Choose your LLM (AI) Provider:")
    print("  1) OpenAI (official API)")
    print("  2) Anthropic (official API)")
    print("  3) OpenRouter")
    print("  4) Local (Ollama, LM Studio)")
    
    choice = prompt_input("Select option (1-4)", default="1")
    
    llm_provider = "openai"
    llm_model = "gpt-4o-mini"
    llm_base_url = ""
    openai_key = ""
    anthropic_key = ""
    openrouter_key = ""
    
    if choice == "2":
        llm_provider = "anthropic"
        llm_model = "claude-3-5-sonnet-20241022"
        print("\n👉 Get an Anthropic API key from: https://console.anthropic.com/")
        anthropic_key = prompt_input("Enter your Anthropic API Key (sk-ant-...)")
    elif choice == "3":
        llm_provider = "openrouter"
        llm_model = "openrouter/auto"
        print("\n👉 Get an OpenRouter API key from: https://openrouter.ai/keys")
        openrouter_key = prompt_input("Enter your OpenRouter API Key (sk-or-...)")
    elif choice == "4":
        llm_provider = "local"
        print("\n👉 Ensure Ollama (https://ollama.com) or LM Studio is running locally.")
        llm_model = prompt_input("Enter your Local Model Name (e.g. deepseek-r1:7b, llama3.2)", default="deepseek-r1:7b")
        llm_base_url = prompt_input("Enter Local API URL", default="http://localhost:11434/v1")
    else:
        print("\n👉 Get an OpenAI API key from: https://platform.openai.com/api-keys")
        openai_key = prompt_input("Enter your OpenAI API Key (sk-...)")

    # 2. News API (Optional)
    print("\n--- 2. News API Key (Optional) ---")
    print("NewsAPI.org key is optional. If left blank, Mosaic will rely solely on")
    print("free Google News RSS feeds.")
    print("👉 Get a free NewsAPI key from: https://newsapi.org/register")
    newsapi_key = prompt_input("Enter NewsAPI.org API Key (leave blank to skip)")

    # 3. Gold API (Optional)
    print("\n--- 3. Gold API Key (Optional) ---")
    print("Gold API provides live COMEX pre-market metal prices (Gold, Silver, Copper).")
    print("👉 Get a free Gold API key from: https://gold-api.com/")
    gold_key = prompt_input("Enter Gold API Key (leave blank or press Enter to use default demo key)", default="your_gold_api_key_here")

    # Read .env.example template
    if not os.path.exists(".env.example"):
        print("ERROR: '.env.example' template not found in current directory.")
        sys.exit(1)
        
    with open(".env.example", "r") as f:
        lines = f.readlines()

    # Modify lines based on settings
    output_lines = []
    for line in lines:
        if line.startswith("OPENAI_API_KEY="):
            output_lines.append(f"OPENAI_API_KEY={openai_key}\n")
        elif line.startswith("ANTHROPIC_API_KEY="):
            output_lines.append(f"ANTHROPIC_API_KEY={anthropic_key}\n")
        elif line.startswith("OPENROUTER_API_KEY="):
            output_lines.append(f"OPENROUTER_API_KEY={openrouter_key}\n")
        elif line.startswith("LLM_PROVIDER="):
            output_lines.append(f"LLM_PROVIDER={llm_provider}\n")
        elif line.startswith("LLM_MODEL="):
            output_lines.append(f"LLM_MODEL={llm_model}\n")
        elif line.startswith("LLM_BASE_URL="):
            output_lines.append(f"LLM_BASE_URL={llm_base_url}\n")
        elif line.startswith("NEWSAPI_KEY="):
            output_lines.append(f"NEWSAPI_KEY={newsapi_key}\n")
        elif line.startswith("GOLD_API_KEY="):
            output_lines.append(f"GOLD_API_KEY={gold_key}\n")
        else:
            output_lines.append(line)

    # Write output to .env
    with open(".env", "w") as f:
        f.writelines(output_lines)

    print("\n=================================================================")
    print(" ✓ Configuration successful! Created '.env' file.")
    print("=================================================================")
    print(f" LLM Provider : {llm_provider}")
    print(f" LLM Model    : {llm_model}")
    if llm_base_url:
        print(f" LLM Base URL : {llm_base_url}")
    print("=================================================================\n")

if __name__ == "__main__":
    run_wizard()
