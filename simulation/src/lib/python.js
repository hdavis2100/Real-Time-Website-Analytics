import { spawn } from "node:child_process";
import path from "node:path";

const pythonBin =
  process.env.PYTHON_BIN ||
  process.env.PYTHON ||
  (process.platform === "win32" ? "python" : "python3");

export async function runPythonModule(moduleName, args = [], options = {}) {
  const cwd = options.cwd || process.cwd();
  const pythonpath = [
    process.env.PYTHONPATH,
    cwd,
    path.join(cwd, "historical"),
    path.join(cwd, "ml_model"),
    path.join(cwd, "simulation")
  ]
    .filter(Boolean)
    .join(path.delimiter);

  return new Promise((resolve, reject) => {
    const child = spawn(pythonBin, ["-m", moduleName, ...args], {
      cwd,
      env: {
        ...process.env,
        PYTHONPATH: pythonpath,
        ...options.env
      }
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", reject);

    child.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `python -m ${moduleName} failed with code ${code}\n${stderr || stdout}`
          )
        );
        return;
      }

      resolve({
        stdout: stdout.trim(),
        stderr: stderr.trim()
      });
    });
  });
}

export function parsePythonJson(output) {
  if (!output?.stdout) {
    return null;
  }

  return JSON.parse(output.stdout);
}
