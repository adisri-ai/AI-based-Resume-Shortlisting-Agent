// frontend/src/api.js

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

// ======================================================
// CACHED STORAGE ACCOUNT
// Prevents repeated backend calls for storage URL
// ======================================================

let cachedStorageAccount = null;

// ======================================================
// GET STORAGE CONFIG
// Called only once and cached afterwards
// ======================================================

export async function getStorageConfig() {
  if (cachedStorageAccount) {
    return cachedStorageAccount;
  }

  const res = await fetch(`${API_BASE_URL.replace("/api", "")}/config/frontend`);

  if (!res.ok) {
    throw new Error("Failed to fetch storage configuration");
  }

  const data = await res.json();

  cachedStorageAccount = data.storageAccount;

  return cachedStorageAccount;
}

// ======================================================
// GET UPLOAD SAS
// ======================================================

export async function getUploadSas(filename) {
  const res = await fetch(`${API_BASE_URL}/api/get-upload-sas`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ filename }),
  });

  if (!res.ok) {
    throw new Error("Failed to get upload SAS");
  }

  return res.json();
}

// ======================================================
// GET RESULTS SAS
// ======================================================

export async function getResultsSas() {
  const res = await fetch(`${API_BASE_URL}/api/get-results-sas`);

  if (!res.ok) {
    throw new Error("Failed to get results SAS");
  }

  return res.json();
}

// ======================================================
// GET LIVE RESULTS JSON
// ======================================================

export async function getResultsJson() {
  const res = await fetch(`${API_BASE_URL}/api/get-results-json`);

  if (!res.ok) {
    throw new Error("Failed to get results JSON");
  }

  return res.json();
}