// src/App.jsx
import React, { useState } from "react";
import { getUploadSas, getResultsSas } from "./api";
import "./App.css";

function App() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState("");
  const [resultUrl, setResultUrl] = useState("");
  const [activeTab, setActiveTab] = useState("upload");
// the remaining code is hidden to maintain project uniqueness.
