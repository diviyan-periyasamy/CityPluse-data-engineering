# # 🚨 CrowdSafe AI — Real-Time Crowd Monitoring & Stampede Detection

An end-to-end **AI-powered crowd safety monitoring system** that uses computer vision to detect people, analyze crowd density and movement, identify high-risk zones, and generate potential stampede-risk alerts.

The project combines **YOLOv8, OpenCV, Streamlit, Flask, MLflow, DVC, Docker, and GitHub Actions** to demonstrate a complete **Computer Vision + MLOps pipeline** from model training to deployment.

---

## 🎯 Project Overview

CrowdSafe AI analyzes video, webcam, DroidCam, or image input in real time to monitor crowd behavior.

The system:

* Detects people using a fine-tuned **YOLOv8 Nano** model
* Estimates crowd density using **Gaussian density heatmaps**
* Analyzes crowd movement using **Farneback Dense Optical Flow**
* Divides the scene into a **4×3 grid** for localized risk analysis
* Calculates zone-level crowd risk scores
* Identifies potentially dangerous crowd conditions
* Generates alerts for high-risk zones
* Provides a real-time **Streamlit dashboard**
* Includes an end-to-end **MLOps pipeline** for experiment tracking, versioning, deployment, and monitoring

---

## ✨ Key Features

### 👥 Real-Time Person Detection

* Fine-tuned **YOLOv8 Nano** object detection model
* Real-time inference on video and webcam streams
* Supports image-based detection
* Bounding-box visualization and person counting

### 🌡️ Crowd Density Analysis

* Gaussian density heatmap generation
* Visual representation of crowd concentration
* Zone-based density analysis
* Helps identify areas with unusually high crowd concentration

### 🌊 Crowd Movement Analysis

Uses **Farneback Dense Optical Flow** to analyze movement between video frames.

The system can identify:

* Movement intensity
* Directional crowd motion
* Sudden changes in movement
* Potentially abnormal movement patterns

### 🗺️ 4×3 Zone-Based Risk Scoring

The monitored area is divided into **12 zones** using a 4×3 grid.

Each zone is evaluated using crowd-related indicators such as:

* People density
* Crowd movement
* Movement intensity
* Localized crowd concentration

The resulting risk score helps identify **high-risk areas within the scene**.

### 🚨 Risk Alerts

The system generates alerts when crowd conditions exceed predefined risk thresholds.

Example:

```text
⚠️ HIGH RISK DETECTED
Zone: B3
Risk Score: 82%
Reason: High crowd density + intense movement
```

### 📊 Streamlit Dashboard

Interactive dashboard supporting:

* Video upload
* Webcam input
* DroidCam input
* Image input
* Real-time detection
* Crowd heatmaps
* Optical-flow visualization
* Risk-zone visualization
* Alert logging
* CSV export

---

# 🧠 System Architecture

```text
                ┌─────────────────────┐
                │   Video / Webcam    │
                │   DroidCam / Image  │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │    YOLOv8 Nano      │
                │  Person Detection   │
                └──────────┬──────────┘
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
      ┌────────────┐ ┌────────────┐ ┌──────────────┐
      │   Crowd    │ │  Optical   │ │    Person    │
      │   Density  │ │    Flow    │ │    Count     │
      │  Heatmap   │ │  Analysis  │ │              │
      └─────┬──────┘ └──────┬─────┘ └──────┬───────┘
            │               │              │
            └───────────────┼──────────────┘
                            ▼
                 ┌─────────────────────┐
                 │   4×3 Risk Grid     │
                 │   Zone Scoring      │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │  Risk Classification│
                 │ Low / Medium / High │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │ Alerts & Dashboard  │
                 │ Streamlit Interface │
                 └─────────────────────┘
```

---

# 🤖 Machine Learning

## Model

**YOLOv8 Nano**

The YOLOv8 Nano model was fine-tuned using a custom crowd dataset sourced from **Roboflow**.

### Training Environment

| Parameter         | Details              |
| ----------------- | -------------------- |
| Model             | YOLOv8 Nano          |
| Dataset           | Custom Crowd Dataset |
| Dataset Source    | Roboflow             |
| Training Platform | Google Colab         |
| GPU               | NVIDIA T4            |
| Training Epochs   | 50                   |
| Task              | Person Detection     |

The trained model is then integrated into the real-time inference pipeline.

---

# 🔬 Computer Vision Pipeline

### 1. Person Detection

YOLOv8 identifies people within each frame.

```text
Input Frame
     ↓
YOLOv8 Inference
     ↓
Person Bounding Boxes
     ↓
Person Count
```

### 2. Density Estimation

Detected person locations are converted into a Gaussian density representation.

```text
Person Locations
       ↓
Gaussian Kernel
       ↓
Density Map
       ↓
Heatmap Visualization
```

### 3. Optical Flow

Farneback Dense Optical Flow estimates pixel-level motion between consecutive frames.

```text
Frame t
   +
Frame t+1
   ↓
Dense Optical Flow
   ↓
Movement Magnitude
   ↓
Movement Analysis
```

### 4. Risk Scoring

The scene is divided into a **4×3 grid**.

Each zone receives a risk score based on crowd-related measurements.

```text
┌─────┬─────┬─────┬─────┐
│ A1  │ A2  │ A3  │ A4  │
├─────┼─────┼─────┼─────┤
│ B1  │ B2  │ B3  │ B4  │
├─────┼─────┼─────┼─────┤
│ C1  │ C2  │ C3  │ C4  │
└─────┴─────┴─────┴─────┘
```

The system can then highlight zones with elevated risk.

---

# ⚙️ MLOps Pipeline

CrowdSafe AI also demonstrates production-oriented MLOps practices.

### 📈 MLflow

Used for:

* Experiment tracking
* Parameter logging
* Metric tracking
* Model artifacts
* Model registry
* Model version management
* Staging and production model versions

### 📦 DVC

Used for:

* Dataset version control
* Model versioning
* Reproducible experiments
* Tracking large ML artifacts

### 🔄 GitHub Actions

Used to automate CI/CD workflows.

Example pipeline:

```text
Git Push
   ↓
GitHub Actions
   ↓
Install Dependencies
   ↓
Run Tests
   ↓
Validate Project
   ↓
Build Docker Image
   ↓
Deployment
```

### 🐳 Docker

The application is containerized for consistent deployment across environments.

### 🌐 Flask REST API

A Flask API provides model-serving endpoints.

Available endpoints:

```text
GET  /health
POST /predict
```

Example:

```bash
curl http://localhost:5000/health
```

### 📉 Drift Detection

The system includes monitoring to identify potential degradation in model performance over time.

---

# 🛠️ Tech Stack

### Programming

* Python

### Machine Learning / Computer Vision

* YOLOv8
* OpenCV
* NumPy

### Application

* Streamlit
* Flask

### MLOps

* MLflow
* DVC
* Docker
* GitHub Actions

### Development

* Git
* GitHub
* Google Colab

---

# 📁 Project Structure

```text
crowdsafe-ai/
│
├── app.py                         # Streamlit dashboard
│
├── src/
│   ├── detection.py               # YOLOv8 inference
│   ├── heatmap.py                 # Gaussian density heatmaps
│   ├── optical_flow.py            # Farneback optical flow
│   └── risk_scoring.py            # Zone-based risk scoring
│
├── api/
│   └── app.py                     # Flask REST API
│
├── mlflow/
│   └── ...                         # MLflow configurations
│
├── .github/
│   └── workflows/
│       └── ...                     # GitHub Actions CI/CD
│
├── Dockerfile                     # Docker configuration
├── requirements.txt               # Python dependencies
├── README.md
└── ...
```

---

# 🚀 Installation & Setup

## 1. Clone the Repository

```bash
git clone https://github.com/DhulakshanKannan/crowdsafe-ai.git
cd crowdsafe-ai
```

## 2. Create a Virtual Environment

```bash
python -m venv venv
```

### Windows

```bash
venv\Scripts\activate
```

### Linux / macOS

```bash
source venv/bin/activate
```

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

## 4. Run the Streamlit Application

```bash
streamlit run app.py
```

The dashboard will be available locally through the Streamlit server.

---

# 🐳 Run with Docker

## Build the Docker Image

```bash
docker build -t crowdsafe-ai .
```

## Run the Container

```bash
docker run -p 8501:8501 crowdsafe-ai
```

Then open the Streamlit application in your browser.

---

# 📡 API Usage

Start the Flask API and use the available endpoints.

### Health Check

```http
GET /health
```

### Prediction

```http
POST /predict
```

The prediction endpoint accepts an input image and returns the model's detection results.

---

# 📊 Project Outputs

CrowdSafe AI produces:

* Real-time person detections
* Person counts
* Crowd density heatmaps
* Optical-flow visualizations
* Zone-level risk scores
* High-risk zone alerts
* Alert logs
* CSV reports
* MLflow experiment records
* Versioned ML artifacts

---

# 🎯 Project Objectives

The main objectives of CrowdSafe AI are to demonstrate how **computer vision and machine learning can be combined with MLOps practices to build a production-oriented safety monitoring system**.

The project focuses on:

* Real-time computer vision
* Crowd behavior analysis
* Risk-zone identification
* Machine learning deployment
* Experiment tracking
* Dataset/model versioning
* CI/CD automation
* Containerization
* API-based model serving
* Model monitoring

---

# 🔮 Future Improvements

Potential future improvements include:

* [ ] Multi-object tracking with ByteTrack/DeepSORT
* [ ] Advanced crowd behavior classification
* [ ] Transformer-based vision models
* [ ] Improved stampede-event detection
* [ ] Real-time notification system
* [ ] Cloud deployment
* [ ] Kubernetes-based deployment
* [ ] Advanced model monitoring
* [ ] Automated model retraining
* [ ] Edge-device deployment
* [ ] Multi-camera crowd monitoring
* [ ] Live monitoring dashboard with historical analytics

---

# ⚠️ Disclaimer

CrowdSafe AI is an **academic and experimental project** designed to demonstrate computer vision, machine learning, and MLOps concepts.

Risk scores and alerts are algorithmic indicators and should **not be treated as a replacement for professional security systems, emergency response procedures, or human supervision**.

---

# 👨‍💻 Author

**Dhulakshan Kannan**

BSc (Hons) Data Science — Coventry University
NIBM | HND Machine Learning

**Project:** CrowdSafe AI — Real-Time Crowd Monitoring & Stampede Detection

### 🔗 Links

* **GitHub:** https://github.com/DhulakshanKannan
* **Project Repository:** https://github.com/DhulakshanKannan/crowdsafe-ai
* **LinkedIn:** Add your LinkedIn profile URL here

---

## ⭐ If you find this project useful

Consider giving the repository a **star ⭐** and following the project for future updates.
