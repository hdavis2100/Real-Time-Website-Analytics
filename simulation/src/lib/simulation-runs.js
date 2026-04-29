import { parsePythonJson, runPythonModule } from "./python.js";

function parseRegistryPayload(output) {
  const payload = parsePythonJson(output);
  if (payload?.found === false) {
    return null;
  }
  return payload;
}

export async function queueSimulationRun(request) {
  const output = await runPythonModule("simulator.run_registry", [
    "enqueue",
    "--payload-json",
    JSON.stringify(request)
  ]);
  return parseRegistryPayload(output);
}

export async function getSimulationRun(simulationRunId) {
  const output = await runPythonModule("simulator.run_registry", [
    "status",
    "--simulation-run-id",
    simulationRunId
  ]);
  return parseRegistryPayload(output);
}

export async function markSimulationRunFailed(simulationRunId, errorMessage) {
  const output = await runPythonModule("simulator.run_registry", [
    "fail",
    "--simulation-run-id",
    simulationRunId,
    "--error-message",
    errorMessage
  ]);
  return parseRegistryPayload(output);
}
