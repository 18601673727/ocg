import { ContractError, type Project, type Projects } from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { type Send } from "./transport";
import { identity } from "./validators";

export type ProjectCommand = {
  name: string;
  argv: string[];
  owner?: "user" | "decision";
  status?: "allowed" | "review" | "blocked";
};

export type ProjectCreate = {
  name: string;
  root: string;
  description?: string | null;
  commands?: ProjectCommand[];
};

export type ProjectUpdate = {
  name: string;
  description: string | null;
  commands: ProjectCommand[];
};

export function createProjectsApi(send: Send) {
  async function readProjectPage(
    settings: { before?: string; limit?: number; signal?: AbortSignal } = {},
  ) {
    const query: string[] = [];
    if (settings.before !== undefined) query.push(`before=${identity(settings.before, "before")}`);
    if (settings.limit !== undefined) {
      if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      query.push(`limit=${settings.limit}`);
    }
    const suffix = query.length ? `?${query.join("&")}` : "";
    return (
      await send("GET", `/api/v1/projects${suffix}`, {
        signal: settings.signal,
      })
    ).input as Projects;
  }

  return {
    readProjects: readProjectPage,

    async readAllProjects(signal?: AbortSignal) {
      const projects: Project[] = [];
      let before: string | undefined;
      for (;;) {
        const page = await readProjectPage({ before, limit: 100, signal });
        projects.push(...page.projects);
        if (!page.has_more) return projects;
        if (!page.next_before || page.next_before === before)
          throw new Error("Project cursor did not advance.");
        before = page.next_before;
      }
    },

    async createProject(body: ProjectCreate, signal?: AbortSignal) {
      const answer = await send("POST", "/api/v1/projects", {
        body,
        sign: true,
        signal,
      });
      return (answer.input as Api.ProjectAnswer).project as Project;
    },

    async updateProject(projectId: string, body: ProjectUpdate, signal?: AbortSignal) {
      identity(projectId, "project");
      const answer = await send("POST", `/api/v1/projects/${projectId}`, {
        body,
        sign: true,
        signal,
      });
      return (answer.input as Api.ProjectAnswer).project as Project;
    },

    async configureProjectDecision(
      projectId: string,
      decisionProfile: string | null,
      minimumConfidence: number,
      enabled: boolean,
      signal?: AbortSignal,
    ) {
      identity(projectId, "project");
      return (
        (
          await send("POST", `/api/v1/projects/${projectId}/decision`, {
            body: {
              enabled,
              decision_profile: decisionProfile,
              minimum_confidence: minimumConfidence,
            },
            sign: true,
            signal,
          })
        ).input as Api.ProjectAnswer
      ).project as Project;
    },
  };
}
