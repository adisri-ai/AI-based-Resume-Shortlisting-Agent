import React, { useState, useEffect, useRef, useCallback } from "react";
import "./index.css";

import {
  Upload,
  Download,
  Search,
  ExternalLink,
  Layers,
  CheckCircle2,
  Loader2,
  FileText,
  BarChart3,
  RefreshCw,
  ChevronDown,
  ChevronUp,
} from "lucide-react";

import { motion, AnimatePresence } from "motion/react";

import {
  getUploadSas,
  getResultsSas,
  getResultsJson,
} from "./api";

import logo from "./resunexus-logo.png";

const scoreColor = (score) => {
  if (score >= 7.5) return "bg-emerald-500";
  if (score >= 5)   return "bg-amber-400";
  return "bg-rose-400";
};

const scoreBadge = (score) => {
  if (score >= 7.5) return "text-emerald-700 bg-emerald-50 border-emerald-200";
  if (score >= 5)   return "text-amber-700  bg-amber-50  border-amber-200";
  return               "text-rose-700   bg-rose-50   border-rose-200";
};

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";


export default function App() {
  const [activeTab, setActiveTab]           = useState("upload");
  const [selectedFile, setSelectedFile]     = useState(null);
  const [message, setMessage]               = useState("");
  const [results, setResults]               = useState([]);
  const [skills, setSkills]                 = useState([]);
  const [searchQuery, setSearchQuery]       = useState("");
  const [processing, setProcessing]         = useState(false);
  const [storageAccount, setStorageAccount] = useState("");
  const [workflowStep, setWorkflowStep]     = useState(1);
  const [expandedRow, setExpandedRow]       = useState(null);
  const [sortConfig, setSortConfig]         = useState({ key: "totalScore", dir: "desc" });


  const [totalCandidates, setTotalCandidates]       = useState(0);
  const [processedCandidates, setProcessedCandidates] = useState(0);

  const pollingRef        = useRef(null);
  const lastRowCountRef   = useRef(0);
  const stableCounterRef  = useRef(0);
  const storageAccountRef = useRef("");
  const storageLoadedRef  = useRef(false);


  const topScrollRef    = useRef(null);
  const bottomScrollRef = useRef(null);

  
  useEffect(() => {
    storageAccountRef.current = storageAccount;
  }, [storageAccount]);

  
  useEffect(() => {
    if (storageLoadedRef.current) return;
    storageLoadedRef.current = true;

    const loadConfig = async () => {
      try {
        const res  = await fetch(`${API_BASE_URL}/config/frontend`);
        const data = await res.json();
        setStorageAccount(data.storageAccount || "");
      } catch (err) {
        console.error("Failed to load frontend config:", err);
      }
    };

    loadConfig();
    return () => clearInterval(pollingRef.current);
  }, []);

  
  useEffect(() => {
    const loadInitialResults = async () => {
      try {
        const data = await getResultsJson();
        if (data?.rows?.length) {
          setResults(data.rows);
          setSkills(data.skills || []);
          setActiveTab("results");
        }
      } catch (_) {}
    };
    loadInitialResults();
  }, []);

  
  const handleFileChange = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setSelectedFile(file);
    setMessage(`Selected: ${file.name}`);
  };

  
  const handleUpload = async () => {
    if (!selectedFile) return;

    try {
      setProcessing(true);

      const isJDUpload = !selectedFile.name.toLowerCase().endsWith(".zip");

      setWorkflowStep(isJDUpload ? 2 : 4);
      setMessage("Uploading file…");

      const { uploadUrl } = await getUploadSas(selectedFile.name);

      await fetch(uploadUrl, {
        method: "PUT",
        headers: { "x-ms-blob-type": "BlockBlob" },
        body: selectedFile,
      });

      setMessage("Upload complete. Processing started.");
      setSelectedFile(null);

      startPolling(isJDUpload);
    } catch (err) {
      console.error(err);
      setProcessing(false);
      setMessage("Upload failed. Please try again.");
    }
  };


  const startPolling = useCallback((isJDUpload) => {
    clearInterval(pollingRef.current);
    stableCounterRef.current = 0;
    lastRowCountRef.current  = 0;

    let jdZeroTicks = 0;
    const initialRowCount = results.length;
    pollingRef.current = setInterval(async () => {
      try {
        const data = await getResultsJson();

    
        let statusData = { total: 0, processed: 0 };
        try {
          const statusRes = await fetch(`${API_BASE_URL}/api/processing-status`);
          statusData      = await statusRes.json();
        } catch (_) {}

        setTotalCandidates(statusData.total || 0);
        setProcessedCandidates(statusData.processed || 0);

      
        const rowsWithUrl = (data.rows || []).map((row) => ({
          ...row,
        
          resumeUrl: row.resumeUrl || `https://${storageAccountRef.current}.blob.core.windows.net/incoming/${encodeURIComponent(row.cvName)}`,
        }));

        setResults(rowsWithUrl);
        setSkills([...(data.skills || [])]);

      
        if (isJDUpload) {
          setWorkflowStep(2);

          if (rowsWithUrl.length === 0) {
            jdZeroTicks += 1;
          } else {
            jdZeroTicks = 0;
          }

          if (jdZeroTicks >= 3) {
            clearInterval(pollingRef.current);
            setProcessing(false);
            setWorkflowStep(3);
            setMessage("JD processed ✓ — now upload your CV ZIP.");
          }
          return;
        }

      
        if (rowsWithUrl.length > 0) {
          setActiveTab("results");
          setWorkflowStep(5);
        }

        
        const newlyAddedRows = rowsWithUrl.length - initialRowCount;
        if (
          statusData.total > 0 &&
          statusData.processed >= statusData.total &&
          newlyAddedRows >= statusData.total
        ) {
          clearInterval(pollingRef.current);
          setProcessing(false);
          setMessage("All resumes graded ✓");
        }
      
      } catch (err) {
        console.error(err);
        clearInterval(pollingRef.current);
        setProcessing(false);
        setMessage("Polling error — check console.");
      }
    }, 5000);
  }, []);


  const handleDownload = async () => {
    try {
      const { downloadUrl } = await getResultsSas();
      window.open(downloadUrl, "_blank");
    } catch (err) {
      console.error(err);
    }
  };


  const handleRefresh = async () => {
    try {
      const data = await getResultsJson();

      let statusData = { total: 0, processed: 0 };
      try {
        const statusRes = await fetch(`${API_BASE_URL}/api/processing-status`);
        statusData      = await statusRes.json();
      } catch (_) {}

      setTotalCandidates(statusData.total || 0);
      setProcessedCandidates(statusData.processed || 0);

      const rowsWithUrl = (data.rows || []).map((row) => ({
        ...row,
        resumeUrl: row.resumeUrl || `https://${storageAccountRef.current}.blob.core.windows.net/incoming/${encodeURIComponent(row.cvName)}`,
      }));
      setResults(rowsWithUrl);
      setSkills([...(data.skills || [])]);
    } catch (err) {
      console.error(err);
    }
  };


  const handleSort = (key) => {
    setSortConfig((prev) =>
      prev.key === key
        ? { key, dir: prev.dir === "desc" ? "asc" : "desc" }
        : { key, dir: "desc" }
    );
  };

  const sortedResults = [...results]
    .filter((r) =>
      r.cvName?.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      const mul = sortConfig.dir === "desc" ? -1 : 1;
      if (sortConfig.key === "totalScore") {
        return mul * ((a.totalScore ?? 0) - (b.totalScore ?? 0));
      }
      if (sortConfig.key === "cvName") {
        return mul * (a.cvName ?? "").localeCompare(b.cvName ?? "");
      }
      const idx = parseInt(sortConfig.key.replace("skill_", ""), 10);
      return mul * ((a.scores?.[idx] ?? 0) - (b.scores?.[idx] ?? 0));
    });


  const workflowSteps = [
    "Upload JD",
    "JD Processing",
    "Upload CV ZIP",
    "CV Processing",
    "Live Results",
  ];

  const SortIcon = ({ colKey }) => {
    if (sortConfig.key !== colKey)
      return <ChevronDown size={12} className="opacity-30 ml-1 inline" />;
    return sortConfig.dir === "desc"
      ? <ChevronDown size={12} className="ml-1 inline text-indigo-500" />
      : <ChevronUp   size={12} className="ml-1 inline text-indigo-500" />;
  };


  const handleTopScroll = (e) => {
    if (bottomScrollRef.current) {
      bottomScrollRef.current.scrollLeft = e.target.scrollLeft;
    }
  };

  const handleBottomScroll = (e) => {
    if (topScrollRef.current) {
      topScrollRef.current.scrollLeft = e.target.scrollLeft;
    }
  };


  return (
    <div className="flex bg-slate-50 min-h-screen font-sans">


      <aside className="w-72 bg-slate-900 text-white flex-col hidden lg:flex flex-shrink-0">
        <div className="p-6 flex items-center gap-3">
          <div className="w-9 h-9 bg-indigo-500 rounded-xl flex items-center justify-center">
            <Layers size={18} />
          </div>
          <span className="font-bold text-lg tracking-tight">ResuNexus</span>
        </div>

        {/* Workflow tracker */}
        <div className="px-4 mt-4">
          <div className="text-xs uppercase tracking-widest text-slate-500 mb-4 font-bold px-2">
            Workflow
          </div>
          <div className="space-y-2">
            {workflowSteps.map((step, index) => {
              const stepNum   = index + 1;
              const active    = workflowStep === stepNum;
              const completed = workflowStep > stepNum;
              return (
                <div
                  key={stepNum}
                  className={`flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-300 ${
                    active
                      ? "bg-indigo-500 text-white shadow-lg shadow-indigo-500/30"
                      : completed
                      ? "bg-emerald-500/15 text-emerald-300"
                      : "bg-slate-800/60 text-slate-500"
                  }`}
                >
                  <div className="flex-shrink-0">
                    {completed ? (
                      <CheckCircle2 size={16} />
                    ) : active ? (
                      <Loader2 size={16} className="animate-spin" />
                    ) : (
                      <div className="w-4 h-4 rounded-full border border-slate-600 flex items-center justify-center">
                        <span className="text-[9px] font-bold">{stepNum}</span>
                      </div>
                    )}
                  </div>
                  <span className="text-sm font-medium leading-snug">
                    {step}
                  </span>
                </div>
              );
            })}
          </div>
        </div>

        {/* Nav tabs */}
        <div className="px-4 mt-8">
          <div className="text-xs uppercase tracking-widest text-slate-500 mb-3 font-bold px-2">
            Navigation
          </div>
          {[
            { id: "upload",  label: "Upload",  icon: <Upload size={15} /> },
            { id: "results", label: "Results", icon: <BarChart3 size={15} /> },
          ].map(({ id, label, icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl mb-1 text-sm font-medium transition-all ${
                activeTab === id
                  ? "bg-white/10 text-white"
                  : "text-slate-400 hover:text-slate-200 hover:bg-white/5"
              }`}
            >
              {icon} {label}
              {id === "results" && results.length > 0 && (
                <span className="ml-auto bg-indigo-500 text-white text-xs font-bold px-2 py-0.5 rounded-full">
                  {results.length}
                </span>
              )}
            </button>
          ))}
        </div>

        <div className="mt-auto p-6 border-t border-slate-800">
          <div className="text-xs text-slate-500">Azure OpenAI Engine Active</div>
          {processing && (
            <div className="mt-2 flex items-center gap-2 text-xs text-indigo-400">
              <Loader2 size={12} className="animate-spin" />
              Processing…
            </div>
          )}
        </div>
      </aside>

      {/* ── MAIN ───────────────────────────────────────────────────────── */}
      <main className="flex-1 flex flex-col min-w-0">

        {/* Header */}
        <header className="h-16 bg-white border-b border-slate-200 px-8 flex items-center justify-between flex-shrink-0">
          <div className="flex items-center gap-4">
            <img src={logo} alt="logo" className="w-9 h-9 rounded-xl object-cover" />
            <div>
              <h1 className="font-bold text-slate-800 leading-none">ResuNexus</h1>
              <p className="text-xs text-slate-400 mt-0.5">JD–CV Intelligence Platform</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            {processing && (
              <span className="text-sm font-medium text-indigo-600 flex items-center gap-2">
                <Loader2 size={14} className="animate-spin" /> Processing…
              </span>
            )}
            {activeTab === "results" && (
              <>
                <button
                  onClick={handleRefresh}
                  className="p-2 text-slate-400 hover:text-slate-700 hover:bg-slate-100 rounded-lg transition-colors"
                  title="Refresh results"
                >
                  <RefreshCw size={16} />
                </button>
                <button
                  onClick={handleDownload}
                  className="flex items-center gap-2 px-4 py-2 bg-slate-900 text-white rounded-lg text-sm font-medium hover:bg-slate-700 transition-colors"
                >
                  <Download size={14} /> Download XLSX
                </button>
              </>
            )}
          </div>
        </header>

        {/* Content */}
        <div className="flex-1 overflow-auto">
          <AnimatePresence mode="wait">

            {/* ── UPLOAD TAB ─────────────────────────────────────────── */}
            {activeTab === "upload" && (
              <motion.div
                key="upload"
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.2 }}
                className="max-w-2xl mx-auto p-8"
              >
                <div className="mb-8">
                  <h2 className="text-2xl font-bold text-slate-900">Upload Document</h2>
                  <p className="text-slate-500 mt-1 text-sm">
                    Upload a JD (PDF) first, then upload your CV pool (ZIP).
                  </p>
                </div>

                <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
                  <label
                    htmlFor="fileUpload"
                    className="block border-2 border-dashed border-slate-200 rounded-xl m-4 p-12 flex flex-col items-center cursor-pointer hover:border-indigo-300 hover:bg-indigo-50/30 transition-all"
                  >
                    <div className="w-16 h-16 rounded-2xl bg-indigo-50 flex items-center justify-center mb-4">
                      {selectedFile
                        ? <FileText size={28} className="text-indigo-500" />
                        : <Upload   size={28} className="text-indigo-400" />}
                    </div>
                    <p className="font-semibold text-slate-800">
                      {selectedFile ? selectedFile.name : "Click to browse files"}
                    </p>
                    <p className="text-slate-400 text-sm mt-1">PDF (JD) or ZIP (CV batch)</p>
                    <input
                      type="file"
                      id="fileUpload"
                      className="hidden"
                      accept=".pdf,.zip"
                      onChange={handleFileChange}
                    />
                  </label>

                  <div className="px-4 pb-4 flex justify-end">
                    <button
                      disabled={!selectedFile || processing}
                      onClick={handleUpload}
                      className="px-6 py-2.5 bg-slate-900 text-white rounded-xl text-sm font-semibold hover:bg-slate-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
                    >
                      {processing
                        ? <><Loader2 size={14} className="animate-spin" /> Processing…</>
                        : <><Upload size={14} /> Upload & Process</>}
                    </button>
                  </div>
                </div>

                <AnimatePresence>
                  {message && (
                    <motion.div
                      key="msg"
                      initial={{ opacity: 0, y: 8 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0 }}
                      className="mt-6 bg-indigo-50 border border-indigo-200 text-indigo-700 rounded-xl px-5 py-4 text-sm font-medium"
                    >
                      {message}
                    </motion.div>
                  )}
                </AnimatePresence>

                <div className="mt-8 grid grid-cols-2 gap-4">
                  {[
                    {
                      icon: <FileText size={18} className="text-indigo-500" />,
                      title: "Step 1 — Upload JD",
                      body: "Upload a PDF job description. The system extracts 10 key skills automatically.",
                    },
                    {
                      icon: <BarChart3 size={18} className="text-emerald-500" />,
                      title: "Step 2 — Upload CVs",
                      body: "Upload a ZIP file containing candidate PDFs. Each resume is graded against the JD.",
                    },
                  ].map(({ icon, title, body }) => (
                    <div key={title} className="bg-white rounded-xl border border-slate-200 p-4 shadow-sm">
                      <div className="flex items-center gap-2 mb-2">
                        {icon}
                        <span className="font-semibold text-slate-800 text-sm">{title}</span>
                      </div>
                      <p className="text-xs text-slate-500 leading-relaxed">{body}</p>
                    </div>
                  ))}
                </div>
              </motion.div>
            )}

            {/* ── RESULTS TAB ────────────────────────────────────────── */}
            {activeTab === "results" && (
              <motion.div
                key="results"
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.2 }}
                className="p-6 h-full flex flex-col"
              >
                {/* Top bar */}
                <div className="flex items-center justify-between mb-5 flex-wrap gap-3">
                  <div>
                    <h2 className="text-xl font-bold text-slate-900">Candidate Results</h2>
                    <p className="text-sm text-slate-400 mt-0.5">
                      {/* REQ 2: show X/Y progress */}
                      {totalCandidates > 0
                        ? `${processedCandidates}/${totalCandidates} candidate${totalCandidates !== 1 ? "s" : ""} graded. Refresh
                        from top-right if not updated live`
                        : `${results.length} candidate${results.length !== 1 ? "s" : ""} graded. Refresh from top-right
                        if not updated live`}
                      {processing && (
                        <span className="ml-2 text-indigo-500 font-medium">
                          · live updating…
                        </span>
                      )}
                    </p>
                  </div>

                  <div className="relative">
                    <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                    <input
                      type="text"
                      placeholder="Search candidates…"
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="pl-9 pr-4 py-2 bg-white border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300 w-56"
                    />
                  </div>
                </div>

                {/* Empty state */}
                {sortedResults.length === 0 && (
                  <div className="flex-1 flex flex-col items-center justify-center text-slate-400">
                    <BarChart3 size={40} className="mb-3 opacity-30" />
                    <p className="font-medium">No results yet</p>
                    <p className="text-sm mt-1">
                      {processing
                        ? "Processing — results will appear here shortly."
                        : "Upload a JD then CV ZIP to begin."}
                    </p>
                  </div>
                )}

                {/* Table with dual scrollbars (REQ 3/4) */}
                {sortedResults.length > 0 && (
                  <div className="flex-1 flex flex-col gap-0">
                    {/* REQ 3: top scrollbar (sticky, always visible) */}
                    <div
                      ref={topScrollRef}
                      onScroll={handleTopScroll}
                      className="overflow-x-auto overflow-y-hidden sticky top-0 z-20 bg-white border-b border-slate-200"
                      style={{ height: "16px" }}
                    >
                      <div style={{ width: `${1200 + skills.length * 130}px`, height: "1px" }} />
                    </div>

                    {/* Table container */}
                    <div
                      ref={bottomScrollRef}
                      onScroll={handleBottomScroll}
                      className="flex-1 overflow-auto rounded-xl border border-slate-200 shadow-sm bg-white"
                    >
                      <table className="w-full text-sm border-collapse">
                        {/* REQ 4: sticky header */}
                        <thead className="sticky top-0 z-10">
                          <tr className="bg-slate-50 border-b border-slate-200">
                            <th className="text-left px-4 py-3 font-semibold text-slate-600 whitespace-nowrap sticky left-0 bg-slate-50 z-10 min-w-[200px]">
                              <button
                                onClick={() => handleSort("cvName")}
                                className="flex items-center hover:text-slate-900"
                              >
                                Candidate <SortIcon colKey="cvName" />
                              </button>
                            </th>

                            {skills.map((skill, i) => (
                              <th
                                key={i}
                                className="px-3 py-3 font-semibold text-slate-600 whitespace-nowrap min-w-[130px] text-center"
                              >
                                <button
                                  onClick={() => handleSort(`skill_${i}`)}
                                  className="flex items-center justify-center w-full hover:text-slate-900 leading-snug"
                                  title={skill}
                                >
                                  <span className="max-w-[110px] truncate text-xs">{skill}</span>
                                  <SortIcon colKey={`skill_${i}`} />
                                </button>
                              </th>
                            ))}

                            <th className="px-4 py-3 font-semibold text-slate-600 whitespace-nowrap sticky right-0 bg-slate-50 z-10 text-center min-w-[110px]">
                              <button
                                onClick={() => handleSort("totalScore")}
                                className="flex items-center justify-center w-full hover:text-slate-900"
                              >
                                Total <SortIcon colKey="totalScore" />
                              </button>
                            </th>
                          </tr>
                        </thead>

                        <tbody>
                          <AnimatePresence initial={false}>
                            {sortedResults.map((row, rowIdx) => {
                              const isExpanded = expandedRow === rowIdx;
                              return (
                                <React.Fragment key={`${row.cvName}-${rowIdx}`}>
                                  <motion.tr
                                    initial={{ opacity: 0, y: 6 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    transition={{ delay: rowIdx * 0.03 }}
                                    onClick={() =>
                                      setExpandedRow(isExpanded ? null : rowIdx)
                                    }
                                    className={`border-b border-slate-100 cursor-pointer transition-colors ${
                                      isExpanded
                                        ? "bg-indigo-50"
                                        : "hover:bg-slate-50"
                                    }`}
                                  >
                                    {/* REQ 5: name links to blob URL */}
                                    <td className="px-4 py-3 sticky left-0 bg-inherit z-10">
                                      <a
                                        href={row.resumeUrl || "#"}
                                        target="_blank"
                                        rel="noreferrer"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          if (!row.resumeUrl) e.preventDefault();
                                        }}
                                        className="flex items-center gap-2 hover:text-indigo-600 transition-colors"
                                      >
                                        <div className="w-7 h-7 rounded-lg bg-slate-100 flex items-center justify-center flex-shrink-0">
                                          <FileText size={13} className="text-slate-500" />
                                        </div>
                                        <span
                                          className="font-medium text-slate-800 max-w-[160px] truncate"
                                          title={row.cvName}
                                        >
                                          {row.cvName}
                                        </span>
                                      </a>
                                    </td>

                                    {(row.scores || []).map((score, si) => (
                                      <td key={si} className="px-3 py-3 text-center">
                                        <div className="flex flex-col items-center gap-1">
                                          <span className="font-semibold text-slate-700 text-xs">
                                            {typeof score === "number"
                                              ? score.toFixed(1)
                                              : "—"}
                                          </span>
                                          <div className="w-12 h-1 bg-slate-100 rounded-full overflow-hidden">
                                            <div
                                              className={`h-full rounded-full ${scoreColor(score)}`}
                                              style={{ width: `${(score / 10) * 100}%` }}
                                            />
                                          </div>
                                        </div>
                                      </td>
                                    ))}

                                    <td className="px-4 py-3 sticky right-0 bg-inherit z-10 text-center">
                                      <span
                                        className={`inline-block px-2.5 py-1 rounded-lg border text-xs font-bold ${scoreBadge(
                                          row.totalScore ?? 0
                                        )}`}
                                      >
                                        {typeof row.totalScore === "number"
                                          ? row.totalScore.toFixed(2)
                                          : "—"}
                                        <span className="font-normal opacity-60">/10</span>
                                      </span>
                                    </td>
                                  </motion.tr>

                                  {/* Expanded skill breakdown */}
                                  <AnimatePresence>
                                    {isExpanded && (
                                      <motion.tr
                                        key={`exp-${rowIdx}`}
                                        initial={{ opacity: 0 }}
                                        animate={{ opacity: 1 }}
                                        exit={{ opacity: 0 }}
                                      >
                                        <td
                                          colSpan={skills.length + 2}
                                          className="px-6 py-4 bg-indigo-50 border-b border-indigo-100"
                                        >
                                          <div className="flex flex-wrap gap-3">
                                            {skills.map((skill, si) => (
                                              <div
                                                key={si}
                                                className="bg-white rounded-lg border border-slate-200 px-3 py-2 text-xs min-w-[140px]"
                                              >
                                                <div
                                                  className="text-slate-500 mb-1 truncate"
                                                  title={skill}
                                                >
                                                  {skill}
                                                </div>
                                                <div className="flex items-center gap-2">
                                                  <div className="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                                                    <div
                                                      className={`h-full rounded-full ${scoreColor(
                                                        row.scores?.[si] ?? 0
                                                      )}`}
                                                      style={{
                                                        width: `${
                                                          ((row.scores?.[si] ?? 0) / 10) * 100
                                                        }%`,
                                                      }}
                                                    />
                                                  </div>
                                                  <span className="font-bold text-slate-700">
                                                    {typeof row.scores?.[si] === "number"
                                                      ? row.scores[si].toFixed(1)
                                                      : "—"}
                                                  </span>
                                                </div>
                                              </div>
                                            ))}

                                            {row.resumeUrl && (
                                              <a
                                                href={row.resumeUrl}
                                                target="_blank"
                                                rel="noreferrer"
                                                onClick={(e) => e.stopPropagation()}
                                                className="flex items-center gap-1.5 bg-slate-900 text-white rounded-lg px-3 py-2 text-xs font-semibold hover:bg-slate-700 transition-colors self-end ml-auto"
                                              >
                                                <ExternalLink size={12} /> Open CV
                                              </a>
                                            )}
                                          </div>
                                        </td>
                                      </motion.tr>
                                    )}
                                  </AnimatePresence>
                                </React.Fragment>
                              );
                            })}
                          </AnimatePresence>
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </motion.div>
            )}

          </AnimatePresence>
        </div>
      </main>
    </div>
  );
}
