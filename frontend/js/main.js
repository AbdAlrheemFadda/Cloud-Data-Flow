/* Main JavaScript Application */

const API_BASE_URL = "http://localhost:5000/api";
let currentFileId = null;

// Initialize on page load
document.addEventListener("DOMContentLoaded", function () {
  checkHealth();
  loadJobs();
});

// Section Navigation
function showSection(sectionName) {
  // Hide all sections
  document.querySelectorAll(".section").forEach((section) => {
    section.classList.remove("active");
  });

  // Remove active class from all nav links
  document.querySelectorAll(".nav-link").forEach((link) => {
    link.classList.remove("active");
  });

  // Show selected section
  const sectionId = `${sectionName}-section`;
  const section = document.getElementById(sectionId);
  if (section) {
    section.classList.add("active");
  }

  // Add active class to clicked nav link
  event.target.classList.add("active");

  // Load data if needed
  if (sectionName === "jobs") {
    loadJobs();
  }
}

// File Upload Handlers
function handleDragOver(e) {
  e.preventDefault();
  e.stopPropagation();
  document.getElementById("dropzone").classList.add("drag-over");
}

function handleDragLeave(e) {
  e.preventDefault();
  e.stopPropagation();
  document.getElementById("dropzone").classList.remove("drag-over");
}

function handleDrop(e) {
  e.preventDefault();
  e.stopPropagation();
  document.getElementById("dropzone").classList.remove("drag-over");

  const files = e.dataTransfer.files;
  if (files.length > 0) {
    handleFileSelect({ target: { files } });
  }
}

function handleFileSelect(e) {
  const files = e.target.files;
  if (files.length > 0) {
    const file = files[0];
    displayFilePreview(file);
  }
}

function displayFilePreview(file) {
  const fileSize = (file.size / 1024 / 1024).toFixed(2);
  document.getElementById("preview-filename").textContent = file.name;
  document.getElementById("preview-size").textContent = `${fileSize} MB`;
  document.getElementById("file-preview").style.display = "block";
  document.getElementById("dropzone").style.display = "none";

  currentFileId = generateFileId();
}

function generateFileId() {
  return "file_" + Date.now() + "_" + Math.random().toString(36).substr(2, 9);
}

// Job Submission
async function submitJob() {
  const fileInput = document.getElementById("file-input");
  const file = fileInput.files[0];

  if (!file) {
    alert("Please select a file");
    return;
  }

  const jobType = document.querySelector(
    'input[name="job-type"]:checked'
  ).value;
  const mlAlgos = Array.from(
    document.querySelectorAll('input[name="ml-algo"]:checked')
  ).map((el) => el.value);

  try {
    updateStatus("Uploading file...", "info");

    // Upload file
    const formData = new FormData();
    formData.append("file", file);
    formData.append("job_type", jobType);

    const uploadResponse = await axios.post(
      `${API_BASE_URL}/upload`,
      formData,
      {
        headers: { "Content-Type": "multipart/form-data" },
      }
    );

    currentFileId = uploadResponse.data.file_id;
    updateStatus("File uploaded successfully", "success");

    // Submit job
    updateStatus("Submitting job...", "info");

    const jobResponse = await axios.post(`${API_BASE_URL}/jobs/submit`, {
      file_id: currentFileId,
      job_type: jobType,
      ml_algorithms: mlAlgos,
    });

    const jobId = jobResponse.data.job_id;
    updateStatus(`Job submitted: ${jobId}`, "success");

    // Clear form
    document.getElementById("file-input").value = "";
    document.getElementById("file-preview").style.display = "none";
    document.getElementById("dropzone").style.display = "block";

    // Show jobs section
    document.querySelector("[onclick=\"showSection('jobs')\"]").click();
  } catch (error) {
    console.error("Job submission error:", error);
    updateStatus("Error submitting job: " + error.message, "error");
  }
}

// Load jobs
async function loadJobs() {
  try {
    const response = await axios.get(`${API_BASE_URL}/jobs/list`);
    const jobs = response.data.jobs || [];

    const jobsList = document.getElementById("jobs-list");

    if (jobs.length === 0) {
      jobsList.innerHTML =
        '<p class="placeholder">No jobs found. Upload a file to start processing.</p>';
      return;
    }

    jobsList.innerHTML = jobs
      .map(
        (job) => `
            <div class="job-item">
                <div class="job-info">
                    <h4>${job.job_id}</h4>
                    <small>File: ${job.file_id}</small>
                    <br/>
                    <small>Submitted: ${new Date(
                      job.submitted_at
                    ).toLocaleString()}</small>
                </div>
                <div class="job-actions">
                    <span class="job-status status-${job.status.toLowerCase()}">${
          job.status
        }</span>
                    <button class="btn btn-secondary" onclick="viewResults('${
                      job.job_id
                    }')">View Results</button>
                </div>
            </div>
        `
      )
      .join("");
  } catch (error) {
    console.error("Error loading jobs:", error);
    updateStatus("Error loading jobs", "error");
  }
}

// View Results
async function viewResults(jobId) {
  try {
    updateStatus("Loading results...", "info");

    const response = await axios.get(`${API_BASE_URL}/results/${jobId}`);
    const results = response.data;

    const resultsContainer = document.getElementById("results-container");

    let html = `
            <div class="result-card">
                <h3>Job ${jobId}</h3>
                <p><strong>Status:</strong> ${results.status}</p>
                <p><strong>Submitted:</strong> ${new Date(
                  results.submitted_at
                ).toLocaleString()}</p>
        `;

    if (results.descriptive_stats) {
      html += displayStats(results.descriptive_stats);
    }

    if (results.ml_results) {
      html += displayMLResults(results.ml_results);
    }

    html += `
                <button class="btn btn-primary" onclick="downloadResults('${jobId}')">Download Results</button>
            </div>
        `;

    resultsContainer.innerHTML = html;
    updateStatus("Results loaded", "success");
  } catch (error) {
    console.error("Error loading results:", error);
    updateStatus("Error loading results", "error");
  }
}

function displayStats(stats) {
  let html = "<h4>Descriptive Statistics</h4>";

  if (stats.basic_stats) {
    html += `
            <div>
                <h5>Data Overview</h5>
                <table class="result-table">
                    <tr>
                        <td><strong>Rows:</strong></td>
                        <td>${stats.basic_stats.rows}</td>
                    </tr>
                    <tr>
                        <td><strong>Columns:</strong></td>
                        <td>${stats.basic_stats.columns}</td>
                    </tr>
                </table>
            </div>
        `;
  }

  if (stats.numerical_stats) {
    html +=
      '<h5>Numerical Statistics</h5><table class="result-table"><tr><th>Column</th><th>Min</th><th>Max</th><th>Mean</th></tr>';
    for (const [col, colStats] of Object.entries(stats.numerical_stats)) {
      html += `
                <tr>
                    <td>${col}</td>
                    <td>${colStats.min?.toFixed(2) || "N/A"}</td>
                    <td>${colStats.max?.toFixed(2) || "N/A"}</td>
                    <td>${colStats.mean?.toFixed(2) || "N/A"}</td>
                </tr>
            `;
    }
    html += "</table>";
  }

  return html;
}

function displayMLResults(mlResults) {
  let html = "<h4>Machine Learning Results</h4>";

  if (Array.isArray(mlResults)) {
    mlResults.forEach((result) => {
      html += `
                <div style="margin-bottom: 1rem; padding: 1rem; background: #f5f5f5; border-radius: 4px;">
                    <h5>${result.algorithm}</h5>
                    <p><strong>Execution Time:</strong> ${
                      result.execution_time?.toFixed(2) || "N/A"
                    }s</p>
            `;

      if (result.metrics) {
        html += "<p><strong>Metrics:</strong></p><ul>";
        for (const [key, value] of Object.entries(result.metrics)) {
          if (typeof value !== "object") {
            html += `<li>${key}: ${
              typeof value === "number" ? value.toFixed(4) : value
            }</li>`;
          }
        }
        html += "</ul>";
      }

      if (result.error) {
        html += `<p style="color: red;"><strong>Error:</strong> ${result.error}</p>`;
      }

      html += "</div>";
    });
  }

  return html;
}

// Search results
async function searchResults() {
  const jobId = document.getElementById("job-search").value;

  if (!jobId.trim()) {
    alert("Please enter a job ID");
    return;
  }

  viewResults(jobId);
}

// Download results
async function downloadResults(jobId) {
  try {
    const format = prompt("Download format (json, csv, excel, pdf):", "json");

    if (!format) return;

    const response = await axios.get(
      `${API_BASE_URL}/results/${jobId}/download?format=${format}`,
      {
        responseType: "blob",
      }
    );

    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", `results_${jobId}.${format}`);
    document.body.appendChild(link);
    link.click();
    link.parentNode.removeChild(link);
  } catch (error) {
    console.error("Download error:", error);
    alert("Error downloading results");
  }
}

// Health check
async function checkHealth() {
  try {
    const response = await axios.get(`${API_BASE_URL}/health`);
    updateStatus("Connected to backend", "success");
  } catch (error) {
    console.error("Health check failed:", error);
    updateStatus("Connecting to backend...", "warning");
  }
}

// Status updates
function updateStatus(message, type = "info") {
  const statusEl = document.getElementById("status-message");
  const indicatorEl = document.getElementById("status-indicator");

  statusEl.textContent = message;

  indicatorEl.style.background =
    type === "success"
      ? "#4caf50"
      : type === "error"
      ? "#f44336"
      : type === "warning"
      ? "#ff9800"
      : "#2196f3";

  console.log(`[${type.toUpperCase()}] ${message}`);
}
