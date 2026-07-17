# 🚀 AI-based Resume Shortlisting Agent

> An AI-powered recruitment platform that automatically evaluates and ranks candidate resumes against a Job Description using Large Language Models.

---
## Project Versioning
> This is **Azure Cloud** version of this project which makes use of Azure Blob Storage and Premium models using Azure OpenAI for better output.
> We also have a free version of this project completely independent of cloud for easy access:
       For access to **Free version** :
      > [Github Link](https://github.com/adisri-ai/AI-based-Resume-Shortlisting-Free-Version)
      > [🌐Demo Link](https://resunexus-frontend.vercel.app/)
## 📖 Overview

Recruiters often spend hours manually reviewing hundreds of resumes for a single job opening.

This project automates that process using **Agentic AI**, allowing recruiters to upload a Job Description and a collection of candidate resumes. The system intelligently extracts skills, evaluates each resume against the JD, and generates an overall score for every candidate.

The application is currently deployed as an **Full stack webapp**, where uploading files to Azure Blob Storage automatically starts the entire processing pipeline.

---

## ✨ Features

- 📄 Upload Job Description (PDF)
- 📦 Upload multiple resumes as a ZIP archive
- 🤖 AI-powered resume grading using OpenAI
- 🎯 Automatic candidate scoring
- 📊 Resume ranking based on JD relevance
- ☁️ Azure Blob Storage integration
- ⚡ Azure Blob Trigger Function for automatic processing
- 📥 Download scored results
- 🐳 Fully containerized using Docker

---

# 🏗️ Architecture

```text
                Upload JD
                     │
                     ▼
         Azure Blob Storage (incoming)
                     │
                     ▼
      Azure Blob Trigger Function Starts
                     │
                     ▼
          OpenAI Resume Evaluation
                     │
                     ▼
      Candidate Scores Generated
                     │
                     ▼
     Results stored in Azure Blob Storage
                     │
                     ▼
      React Frontend displays rankings
```

---

# 🛠 Tech Stack

| Category | Technology |
|-----------|------------|
| 🤖 LLM API | OpenAI |
| ☁️ Cloud Platform | Microsoft Azure |
| ⚡ Compute | Azure Functions |
| 📦 Storage | Azure Blob Storage |
| 🔐 Storage Access | Azure Blob SAS |
| 💻 Frontend | ReactJS |
| 🐳 Containerization | Docker |

---

# 📂 Workflow

1. Upload a **Job Description (PDF)**.
2. Upload candidate resumes as a **ZIP** archive.
3. Azure Blob Storage receives the files.
4. Azure Blob Trigger Function is automatically invoked.
5. OpenAI extracts and evaluates candidate skills.
6. Every resume receives a relevance score.
7. Results are displayed in the frontend and can be downloaded.

---

# ⚙️ Requirements

## Software

- Docker Desktop

Install Docker Desktop to pull and run the Docker image locally.

---

## Azure Resources

You must have an active Microsoft Azure subscription with the following services:

### Azure Blob Storage

Used for:

- Uploading Job Descriptions
- Uploading Candidate Resumes
- Storing generated results

### Azure OpenAI Service

Required for:

- Resume classification
- Skill extraction
- Candidate scoring

You will need:

- Azure OpenAI Endpoint
- Azure OpenAI API Key

---

# 🐳 Running with Docker

Pull the latest Docker image

```bash
docker pull <your-dockerhub-username>/<repository>:latest
```

Run the container

```bash
docker run -p 3000:3000 -p 8000:8000 <your-dockerhub-username>/<repository>:latest
```

---

# 📊 Current Capabilities

- ✅ Job Description upload
- ✅ Resume ZIP upload
- ✅ Resume parsing
- ✅ AI-based candidate scoring
- ✅ Automatic Azure processing
- ✅ Results generation
- ✅ React dashboard
- ✅ Docker deployment

---


---

# 📄 License

This project is intended for educational and research purposes.

---

# 👨‍💻 Author

Developed as an AI-powered recruitment automation platform using **Azure**, **OpenAI**, **React**, and **Docker**.
