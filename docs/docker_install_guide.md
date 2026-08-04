# One-Click Setup Guide (Docker)

This guide walks you through setting up and running the Mosaic Fund Agent platform on **Windows**, **macOS**, and **Ubuntu (Linux)** using Docker. This method packages all python libraries and database dependencies automatically, so you do not need to install Python, compilers, or local packages.

---

## 🪟 Windows Setup

### 1. Install Docker Desktop
1. Download the installer from **[Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)**.
2. Run the installer. Ensure **"Use WSL 2 instead of Hyper-V (recommended)"** is checked when prompted.
3. If prompted, restart your computer to finish the WSL2 integration.
4. Launch **Docker Desktop** from your Start menu and accept the agreement. Keep the application open.

### 2. Configure & Run
1. Open the project folder (`data_importer`) in Windows Explorer.
2. Double-click the file **`run.bat`**.
   - *First-run only:* The script will create a file named `.env` in the root folder and then exit.
3. Open the new **`.env`** file using Notepad, fill in your API keys (e.g. `OPENAI_API_KEY`, Zerodha keys, etc.), and save it.
4. Double-click **`run.bat`** again. It will build the container, configure ClickHouse, and open the Streamlit dashboard automatically in your browser at:
   **http://localhost:8501/**

### 3. Stop
When you are done, double-click **`stop.bat`** to shut down the background containers.

---

## 🍎 macOS Setup

### 1. Install Docker Desktop
1. Download the installer for your chip:
   - **[Docker Desktop for Mac (Apple Silicon / M1/M2/M3)](https://desktop.docker.com/mac/main/arm64/Docker.dmg)**
   - **[Docker Desktop for Mac (Intel Chip)](https://desktop.docker.com/mac/main/amd64/Docker.dmg)**
2. Open the downloaded `.dmg` file, drag **Docker** to your Applications folder, and launch it.
3. Accept the license agreement and wait for the status indicator in the bottom-left corner to turn green.

### 2. Configure & Run
1. Open terminal and navigate to the project directory:
   ```bash
   cd /path/to/data_importer
   ```
2. Run the startup script:
   ```bash
   ./run.sh
   ```
   - *First-run only:* This script will create a `.env` file in the folder and prompt you to configure it.
3. Open the **`.env`** file in your text editor, add your API credentials, and save it.
4. Run the script again:
   ```bash
   ./run.sh
   ```
   This will automatically build the images, start the containers, and launch the dashboard in your default browser.

### 3. Stop
To turn off the services, run:
```bash
./stop.sh
```

---

## 🐧 Ubuntu (Linux) Setup

### 1. Install Docker & Docker Compose
Open your terminal and run the following command block to install Docker Engine:
```bash
# Update package index and install certificates
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

# Add Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Set up the repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine and Compose
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Add your user to the docker group so you can run docker commands without sudo
sudo usermod -aG docker $USER
```
*Note: After adding your user to the group, close your terminal window and open a new one (or run `newgrp docker`) for the changes to apply.*

### 2. Configure & Run
1. Navigate to the project directory:
   ```bash
   cd /path/to/data_importer
   ```
2. Run the startup script:
   ```bash
   ./run.sh
   ```
   - *First-run only:* The script will copy `.env.example` to `.env` and exit.
3. Edit the newly created **`.env`** file to add your API credentials.
4. Run the script again:
   ```bash
   ./run.sh
   ```
   It will start the database and Streamlit UI, opening **http://localhost:8501/** when live.

### 3. Stop
To stop all containers, run:
```bash
./stop.sh
```

---

## 💻 Running Commands / Custom Scripts (All Platforms)

To run specific analysis commands or scripts directly in the Docker container (without needing a local Python setup), run the CLI proxy wrapper in your terminal:

| Operation | macOS / Ubuntu / WSL Bash | Windows CMD |
| :--- | :--- | :--- |
| Run COMEX pre-market | `./mosaic.sh comex` | `mosaic.bat comex` |
| Ask the LLM Portfolio Agent | `./mosaic.sh ask "what is my riskiest holding?"` | `mosaic.bat ask "what is my riskiest holding?"` |
| Analyze active portfolio | `./mosaic.sh analyze --max 3` | `mosaic.bat analyze --max 3` |
| Run GOLDBEES Report | `./mosaic.sh src/scripts/goldbees_report.py` | `mosaic.bat src/scripts/goldbees_report.py` |
