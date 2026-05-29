#!/bin/bash
# ── Ollama / Gemma 4 26B Optimization Script for macOS ────────────────────────
#
# This script applies the following optimizations:
# 1. Configures global macOS launchctl environment variables for Ollama (KV cache quantization & Flash Attention).
# 2. Creates a persistent LaunchAgent plist to load these environment variables automatically at startup.
# 3. Compiles the gemma4-26b:latest Ollama model using the optimized Modelfile.
# 4. Informs the user how to temporarily and permanently raise the macOS GPU VRAM limit.

# Formatting colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=================================================================${NC}"
echo -e "${YELLOW}           Ollama Gemma 4 26B macOS Optimization Utility        ${NC}"
echo -e "${BLUE}=================================================================${NC}"

# 1. Apply current-session environment variables via launchctl
echo -e "\n${YELLOW}Step 1: Setting launchctl environment variables...${NC}"
launchctl setenv OLLAMA_KV_CACHE_TYPE q8_0
launchctl setenv OLLAMA_FLASH_ATTENTION 1
echo -e "${GREEN}✓ Done. Current GUI session now has OLLAMA_KV_CACHE_TYPE=q8_0 and OLLAMA_FLASH_ATTENTION=1.${NC}"

# 2. Create persistent LaunchAgent plist for reboots
PLIST_PATH="$HOME/Library/LaunchAgents/com.user.ollama-env.plist"
echo -e "\n${YELLOW}Step 2: Creating persistent LaunchAgent plist...${NC}"
mkdir -p "$(dirname "$PLIST_PATH")"

cat <<EOF > "$PLIST_PATH"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.user.ollama-env</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/sh</string>
        <string>-c</string>
        <string>launchctl setenv OLLAMA_KV_CACHE_TYPE q8_0 && launchctl setenv OLLAMA_FLASH_ATTENTION 1</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
EOF

# Load the LaunchAgent
launchctl load "$PLIST_PATH" 2>/dev/null || launchctl bootstrap gui/"$(id -u)" "$PLIST_PATH" 2>/dev/null
echo -e "${GREEN}✓ Done. Persistent plist created at ${PLIST_PATH} and loaded.${NC}"

# 3. Compile/Rebuild the model in Ollama
echo -e "\n${YELLOW}Step 3: Rebuilding gemma4-26b:latest with optimized Modelfile...${NC}"
if command -v ollama >/dev/null 2>&1; then
    echo "Creating model 'gemma4-26b:latest' from Modelfile. This should be quick..."
    ollama create gemma4-26b:latest -f Modelfile
    echo -e "${GREEN}✓ Done. Model gemma4-26b:latest updated with 32k context and optimal parameters.${NC}"
else
    echo -e "${RED}✗ Error: 'ollama' command line tool not found in PATH.${NC}"
    echo "Please ensure Ollama is installed and run 'ollama create gemma4-26b:latest -f Modelfile' manually."
fi

# 4. Instructions for GPU allocation limit
echo -e "\n${YELLOW}Step 4: Metal GPU VRAM Limit Guidance (Important!)${NC}"
echo "By default, macOS limits Metal to ~66-75% of system RAM (about 24-27 GB on your 36 GB Mac)."
echo "Since Gemma-4-26B (Q6_K) is 21.1 GB and its 32k context q8_0 KV cache is ~7 GB, total VRAM required is ~29 GB."
echo "If VRAM limit is exceeded, macOS will page to CPU RAM, dropping generation speed to < 2 tok/s."
echo ""
echo -e "To temporarily raise the GPU limit to ${GREEN}30 GB (30720 MB)${NC} for this boot session, run:"
echo -e "${BLUE}  sudo sysctl iogpu.wired_limit_mb=30720${NC}"
echo ""
echo "To make this setting permanent across reboots, add it to /etc/sysctl.conf:"
echo -e "${BLUE}  echo \"iogpu.wired_limit_mb=30720\" | sudo tee -a /etc/sysctl.conf${NC}"
echo ""

# 5. Restart advice
echo -e "${YELLOW}Step 5: Restart Ollama.app to apply changes${NC}"
echo "Ollama must be restarted to inherit the new environment variables and VRAM limits."
echo "1. Quit Ollama.app using the menu bar icon (or run: killall Ollama)."
echo "2. Open Ollama.app again from Applications, or run 'ollama serve' in a new terminal window."
echo ""
echo -e "${GREEN}Optimization setup complete!${NC}"
echo -e "${BLUE}=================================================================${NC}"
